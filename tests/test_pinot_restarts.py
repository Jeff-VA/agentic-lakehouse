"""Pinot keeps answering after the stack restarts: its processes register under their service
names, and setup moves segments off servers that registered under a container IP and are gone."""

from pathlib import Path

import httpx
import pytest
import yaml

from agentic_lakehouse.orchestrator import LOST, reassign_lost_segments
from agentic_lakehouse.scaffold import TEMPLATE

ROOT = Path(__file__).parents[1]
TAGS = ["DefaultTenant_OFFLINE", "DefaultTenant_REALTIME"]


@pytest.mark.parametrize("stack", [ROOT / "docker" / "docker-compose.yml", TEMPLATE / "docker" / "docker-compose.yml"], ids=["engine", "template"])
def test_pinot_registers_under_service_names_not_container_ips(stack):
    """A container can get a new IP when the stack restarts; its service name stays."""
    services = yaml.safe_load(stack.read_text())["services"]
    for service, flag in (("pinot-controller", "-controllerHost"), ("pinot-broker", "-brokerHost"), ("pinot-server", "-serverHost")):
        command = services[service]["command"].split()
        assert command[command.index(flag) + 1] == service


class Controller:
    """As much of a Pinot controller's REST API as reassign_lost_segments uses. Like Pinot's, a
    rebalance assigns segments to every server with the tenant's tag, live or not."""

    def __init__(self, servers: dict[str, list[str]], live: set[str], ideal: dict[str, dict[str, dict[str, str]]]):
        self.servers, self.live, self.ideal = servers, live, ideal
        self.changes: list[str] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        params, parts = dict(request.url.params), request.url.path.strip("/").split("/")
        match request.method, parts:
            case "GET", ["cluster", "info"]:
                return httpx.Response(200, json={"clusterName": "PinotCluster"})
            case "GET", ["zk", "ls"] if params["path"] == "/PinotCluster/LIVEINSTANCES":
                return httpx.Response(200, json=sorted(self.live | {"Broker_pinot-broker_8099"}))
            case "GET", ["instances"]:
                return httpx.Response(200, json={"instances": ["Controller_pinot-controller_9000", *self.servers]})
            case "GET", ["tables", table, "idealstate"]:
                return httpx.Response(200, json={"OFFLINE": None, "REALTIME": self.ideal.get(table)})
            case "PUT", ["instances", name, "updateTags"]:
                self.servers[name] = params["tags"].split(",")
                self.changes.append(f"tag {name} {params['tags']}")
                return httpx.Response(200, json={"status": "updated"})
            case "POST", ["tables", table, "rebalance"]:
                assert params == {"type": "REALTIME", "downtime": "true", "includeConsuming": "true", "bootstrap": "true", "reassignInstances": "true"}
                targets = sorted(name for name, tags in self.servers.items() if "DefaultTenant_REALTIME" in tags)
                self.ideal[table] = {segment: {targets[i % len(targets)]: state for state in replicas.values()} for i, (segment, replicas) in enumerate(sorted(self.ideal[table].items()))}
                self.changes.append(f"rebalance {table}")
                return httpx.Response(200, json={"status": "DONE"})
            case "DELETE", ["instances", name]:
                if any(name in replicas for segments in self.ideal.values() for replicas in segments.values()):
                    return httpx.Response(409, json={"error": f"Instance {name} exists in ideal state"})
                del self.servers[name]
                self.changes.append(f"remove {name}")
                return httpx.Response(200, json={"status": "Successfully dropped instance"})
        raise AssertionError(f"unexpected {request.method} {request.url}")

    def repair(self, tables: list[str]) -> list[str]:
        logged = []
        with httpx.Client(transport=httpx.MockTransport(self.handle)) as client:
            reassign_lost_segments(client, "http://controller", tables, logged.append)
        return logged


def test_segments_on_a_server_lost_with_its_ip_move_to_the_live_server():
    """The stack restarted twice, and the server came back as pinot-server: the table's segments are
    still assigned to its first IP, and another lost name still has the tenant's tag."""
    controller = Controller(
        servers={"Server_172.21.0.6_8098": TAGS, "Server_172.21.0.16_8098": TAGS, "Server_pinot-server_8098": TAGS},
        live={"Server_pinot-server_8098"},
        ideal={"telemetry": {"telemetry__0__0__20261109T2006Z": {"Server_172.21.0.6_8098": "CONSUMING"}}, "other": {}},
    )
    logged = controller.repair(["telemetry", "other"])
    assert controller.ideal["telemetry"] == {"telemetry__0__0__20261109T2006Z": {"Server_pinot-server_8098": "CONSUMING"}}
    assert controller.servers == {"Server_pinot-server_8098": TAGS}
    assert controller.changes == [
        f"tag Server_172.21.0.6_8098 {LOST}", f"tag Server_172.21.0.16_8098 {LOST}", "rebalance telemetry",
        "remove Server_172.21.0.6_8098", "remove Server_172.21.0.16_8098",
    ]  # fmt: skip
    assert logged == [
        "pinot: moving table telemetry's segments off Server_172.21.0.6_8098, gone since its IP changed (DONE)",
        "pinot: removed Server_172.21.0.6_8098, gone since its IP changed",
        "pinot: removed Server_172.21.0.16_8098, gone since its IP changed",
    ]
    assert (controller.repair(["telemetry", "other"]), controller.changes[5:]) == ([], [])  # nothing left to do


@pytest.mark.parametrize(
    ("servers", "live"),
    [
        ({"Server_pinot-server_8098": TAGS}, {"Server_pinot-server_8098"}),  # all well
        ({"Server_pinot-server_8098": TAGS}, set()),  # restarting: it keeps its name
        ({"Server_172.21.0.6_8098": TAGS}, set()),  # a stack from before 0.10.2, restarting
        ({"Server_pinot-server-1_8098": TAGS, "Server_pinot-server-2_8098": TAGS}, {"Server_pinot-server-2_8098"}),  # one of two restarting
    ],
)
def test_servers_named_by_service_or_with_no_live_one_to_take_over_are_left_alone(servers, live):
    ideal = {"telemetry": {"telemetry__0__0__20261109T2006Z": {name: "CONSUMING"} for name in list(servers)[:1]}}
    controller = Controller(servers=dict(servers), live=live, ideal=ideal)
    assert (controller.repair(["telemetry"]), controller.changes) == ([], [])
