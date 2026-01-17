import argparse
import logging
import os
from rexec_broker.broker import RExecBroker

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--client_port", type=str, default="5559",
        help="The port for listening the clients' requests. [0-65535]"
    )

    parser.add_argument(
        "--server_port", type=str, default="5560",
        help="The port for listening the servers' requests. [0-65535]"
    )

    parser.add_argument(
        "--control_port", type=str, default="5561",
        help="The port for listening the termination signal. [0-65535]"
    )

    parser.add_argument(
        "--auth_api_url", type=str, default=os.environ.get("AUTH_API_URL"),
        help="Auth API URL for token validation."
    )

    parser.add_argument(
        "-v", "--verbose",
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
    )

    parser.add_argument(
        "--debug",
        help="Show debug info",
        action="store_const", dest="loglevel", const=logging.DEBUG,
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    broker = RExecBroker(args)
    broker.run()
