"""The drift agent's parts. agentic_lakehouse/agent_sync.py runs them; the design is in
docs/design/drift-agent.md.

- signals.py: what a dataset's data shows that its config may not reflect yet
- proposal.py: what Claude can propose, as structured output
- patch.py: a proposal applied to the config files' text, and the diff for review
"""
