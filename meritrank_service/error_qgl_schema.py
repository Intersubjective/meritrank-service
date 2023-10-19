import traceback

import strawberry
from graphql import GraphQLError, ExecutionContext

from meritrank_service.log import LOGGER


class ErrorEnabledSchema(strawberry.Schema):
    def process_errors(
            self,
            errors: "list[GraphQLError]",
            execution_context: "ExecutionContext" = None,
    ) -> None:
        super().process_errors(errors, execution_context)

        for error in errors:
            err = getattr(error, "original_error")
            if err:
                tb_str = "".join(traceback.format_exception(type(err), err, err.__traceback__))
                LOGGER.error(tb_str)
