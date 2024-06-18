from dagster import Config, OpExecutionContext, op
from dagster_slack import SlackResource


class MessageConfig(Config):
    message: str


@op
def send_slack_message(
    context: OpExecutionContext,
    slack_resource: SlackResource,
    config: MessageConfig,
):
    message = config.message
    slack_resource.get_client().chat_postMessage(channel="D079849PJ3S", text=message)
