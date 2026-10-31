import asyncio
import logging
import sys
from types import MappingProxyType
from typing import Annotated, Literal, Mapping

import mcp.types as types
from mcp.server import Server
from mcp.server.context import ServerRequestContext
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ConfigDict, Field, ValidationError

logger = logging.getLogger(__name__)

CustomerId = Annotated[
    str, Field(min_length=10, max_length=10, pattern=r"^CUST-[0-9]{5}$")
]
Amount = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
Reason = Annotated[str, Field(min_length=10)]


class CustomerArgs(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    customer_id: CustomerId


class RefundArgs(CustomerArgs):
    amount: Amount
    reason: Reason


class CustomerRecord(CustomerArgs):
    name: str
    mock: Literal[True] = True


class MockRefundReceipt(RefundArgs):
    status: Literal["mock_refund_simulated"] = "mock_refund_simulated"
    mock: Literal[True] = True
    payment_processed: Literal[False] = False


class BusinessFailure(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    code: Literal["CUSTOMER_NOT_FOUND"] = "CUSTOMER_NOT_FOUND"
    message: str = "No mock customer matches this customer_id."
    mock: Literal[True] = True


CUSTOMERS: Mapping[str, CustomerRecord] = MappingProxyType(
    {
        "CUST-00001": CustomerRecord(customer_id="CUST-00001", name="Alex Morgan"),
        "CUST-00002": CustomerRecord(customer_id="CUST-00002", name="Sam Rivera"),
    }
)


def get_customer_record(args: CustomerArgs) -> CustomerRecord | BusinessFailure:
    return CUSTOMERS.get(args.customer_id, BusinessFailure())


def trigger_refund(args: RefundArgs) -> MockRefundReceipt | BusinessFailure:
    if args.customer_id not in CUSTOMERS:
        return BusinessFailure()
    return MockRefundReceipt(
        customer_id=args.customer_id, amount=args.amount, reason=args.reason
    )


async def _list_tools(
    ctx: ServerRequestContext, params: types.PaginatedRequestParams | None
) -> types.ListToolsResult:
    return types.ListToolsResult(
        tools=[
            types.Tool(
                name="get_customer_record",
                description="Look up an immutable mock customer record. No external system is queried.",
                input_schema=CustomerArgs.model_json_schema(),
            ),
            types.Tool(
                name="trigger_refund",
                description="Simulate a refund for a mock customer. No payment is processed or persisted.",
                input_schema=RefundArgs.model_json_schema(),
            ),
        ]
    )


async def _call_tool(
    ctx: ServerRequestContext, params: types.CallToolRequestParams
) -> types.CallToolResult:
    try:
        match params.name:
            case "get_customer_record":
                args = CustomerArgs.model_validate(params.arguments)
            case "trigger_refund":
                args = RefundArgs.model_validate(params.arguments)
            case _:
                raise MCPError(types.INVALID_PARAMS, "Unknown tool")
    except ValidationError as exc:
        details = [
            {"path": list(error["loc"]), "type": error["type"], "message": error["msg"]}
            for error in exc.errors(include_input=False, include_context=False, include_url=False)
        ]
        logger.warning("Rejected invalid arguments for %s", params.name)
        raise MCPError(types.INVALID_PARAMS, "Invalid tool arguments", details) from None

    try:
        result = (
            trigger_refund(args)
            if isinstance(args, RefundArgs)
            else get_customer_record(args)
        )
        return types.CallToolResult(
            content=[types.TextContent(text=result.model_dump_json())],
            structured_content=result.model_dump(mode="json"),
            is_error=isinstance(result, BusinessFailure),
        )
    except Exception:
        logger.exception("Tool execution failed for %s", params.name)
        raise MCPError(types.INTERNAL_ERROR, "Tool execution failed") from None


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s %(message)s")
    server = Server("assessment-task1", version="0.1.0", on_list_tools=_list_tools, on_call_tool=_call_tool)
    logger.info("Starting mock customer and refund MCP server")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
