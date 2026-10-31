import asyncio
import json
import sys

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.exceptions import MCPError


async def main() -> None:
    server = StdioServerParameters(command=sys.executable, args=["-m", "task1_mcp.server"])
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream, read_timeout_seconds=10) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("Tools:", ", ".join(tool.name for tool in tools.tools))
            for name, arguments in [
                ("get_customer_record", {"customer_id": "CUST-00001"}),
                (
                    "trigger_refund",
                    {"customer_id": "CUST-00001", "amount": 25.5, "reason": "Duplicate charge"},
                ),
            ]:
                result = await session.call_tool(name, arguments)
                print(name, json.dumps(result.structured_content))
            try:
                await session.call_tool(
                    "trigger_refund",
                    {"customer_id": "CUST-00001", "amount": "25.5", "reason": "Duplicate charge"},
                )
            except MCPError as exc:
                if exc.code != -32602:
                    raise
                print(f"Invalid amount: JSON-RPC {exc.code}, {exc.message}")
            else:
                raise RuntimeError("The server accepted a numeric string")


if __name__ == "__main__":
    asyncio.run(main())
