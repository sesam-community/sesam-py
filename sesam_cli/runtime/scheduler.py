import os
import time
from threading import Thread

from sesam_cli.runtime.performance import profile_phase


def execute_run_internal_scheduler(client):
    start_time = time.monotonic()

    zero_runs = client.args.scheduler_zero_runs
    max_runs = client.args.scheduler_max_runs
    max_run_time = client.args.scheduler_max_run_time
    delete_input_datasets = not os.path.isdir("testdata")
    check_input_pipes = client.args.scheduler_check_input_pipes
    output_run_statistics = client.args.output_run_statistics
    scheduler_mode = client.args.scheduler_mode
    requests_mode = client.args.scheduler_request_mode
    reset_pipes_and_delete_sink_datasets = (
        client.args.scheduler_dont_reset_pipes_or_delete_sink_datasets is not True
    )

    if scheduler_mode is not None and scheduler_mode not in ["active", "poll"]:
        raise RuntimeError("'scheduler_mode' can only be set to 'active' or 'poll'")

    if requests_mode is not None and requests_mode not in ["sync", "async"]:
        raise RuntimeError("'request_mode' can only be set to 'sync' or 'async'")

    class SchedulerRunner(Thread):
        def __init__(self, sesam_node):
            super().__init__()
            self.sesam_node = sesam_node
            self.status = None
            self.token = None
            self.additional_info = None
            self.result = {}

        def run(self):
            try:
                self.result = self.sesam_node.run_internal_scheduler(
                    max_run_time=max_run_time,
                    max_runs=max_runs,
                    zero_runs=zero_runs,
                    delete_input_datasets=delete_input_datasets,
                    check_input_pipes=check_input_pipes,
                    output_run_statistics=output_run_statistics,
                    scheduler_mode=scheduler_mode,
                    request_mode=requests_mode,
                    reset_pipes_and_delete_sink_datasets=reset_pipes_and_delete_sink_datasets,
                )

                if requests_mode == "sync":
                    if self.result["status"] == "success":
                        self.status = "finished"
                    else:
                        self.status = "failed"
                else:
                    # In async mode we loop until status changes
                    # (or status request fails)
                    if "token" not in self.result:
                        raise AssertionError(
                            "Response from scheduler with 'async' "
                            "request_mode didn't contain a token!"
                        )

                    self.token = self.result["token"]
                    while True:
                        # IS-15613: long-running CI tests are also user interactions
                        self.sesam_node.register_user_interaction()

                        status = self.sesam_node.get_internal_scheduler_status(self.token)

                        if status["status"] == "success":
                            self.status = "finished"
                            break
                        elif status["status"] == "failed":
                            self.status = "failed"
                            break
                        elif status["status"] == "not-running":
                            self.status = "failed"
                            self.result = "Scheduler is not running"
                            break

                        time.sleep(10)

            except BaseException as e:
                self.status = "failed"
                self.result = e

    with profile_phase(client, "scheduler.start"):
        scheduler_runner = SchedulerRunner(client.sesam_node)
        scheduler_runner.start()

    time.sleep(1)

    since = None

    def print_internal_scheduler_log(since_val, token=None):
        log_lines = client.sesam_node.get_internal_scheduler_log(since=since_val, token=token)
        for log_line in log_lines:
            if isinstance(log_line, dict):
                msg = "%s - %s - %s" % (
                    log_line["timestamp"],
                    log_line["loglevel"],
                    log_line["logdata"],
                )
                client.logger.info(msg)
            else:
                client.logger.debug(f"Log line was not a dict! Was {type(log_line)} ('{log_line}')")
                return None

        if len(log_lines) > 0:
            return log_lines[-1]["timestamp"]

        return since_val

    with profile_phase(client, "scheduler.wait"):
        while True:
            if client.args.print_scheduler_log is True:
                since = print_internal_scheduler_log(since, token=scheduler_runner.token)

            if scheduler_runner.status is not None:
                break

            time.sleep(1)

    if scheduler_runner.status == "failed":
        client.logger.error("Failed to run pipes to completion")
        if client.args.print_scheduler_log is True:
            print_internal_scheduler_log(since, token=scheduler_runner.token)
        raise RuntimeError(scheduler_runner.result)

    if client.args.print_scheduler_log is True:
        print_internal_scheduler_log(since, token=scheduler_runner.token)

    client.logger.info(
        "Successfully ran all pipes to completion in %s seconds"
        % int(time.monotonic() - start_time)
    )

    additional_info = scheduler_runner.result.get("additional_info")
    if additional_info is not None:
        client.logger.info(additional_info)
        return additional_info

    return None
