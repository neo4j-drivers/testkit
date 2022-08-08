from .. import protocol
from .exceptions import ApplicationCodeError
from .transaction import Transaction


def handle_retry_func(retry_func, res, driver):
    assert isinstance(res, protocol.RetryFunc)
    if not retry_func:
        raise Exception("No retry function was registered")
    res = retry_func(res.exception, res.attempt, res.max_attempts)
    try:
        retry, delay_ms = res
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "retry_func must return retry(bool), delay_ms(int)"
        ) from exc
    driver.send(
        protocol.RetryFuncResult(retry=bool(retry), delay_ms=int(delay_ms))
    )


def run_tx_loop(fn, req, driver, retry_func=None, hooks=None):
    driver.send(req, hooks=hooks)
    x = None
    while True:
        res = driver.receive(hooks=hooks, allow_resolution=True)
        if isinstance(res, protocol.RetryableTry):
            retryable_id = res.id
            tx = Transaction(driver, retryable_id)
            try:
                # Invoke the frontend test function until we succeed, note
                # that the frontend test function makes calls to the
                # backend itself.
                x = fn(tx)
            except (ApplicationCodeError, protocol.DriverError) as e:
                # If this is an error originating from the driver in the
                # backend, retrieve the id of the error  and send that,
                # this saves us from having to recreate errors on backend
                # side, backend just needs to track the returned errors.
                error_id = ""
                if isinstance(e, protocol.DriverError):
                    error_id = e.id
                driver.send(
                    protocol.RetryableNegative(retryable_id,
                                               error_id=error_id),
                    hooks=hooks
                )
            except Exception as e:
                # If this fails any other way, we still want the backend
                # to rollback the transaction.
                try:
                    res = driver.send_and_receive(
                        protocol.RetryableNegative(retryable_id),
                        allow_resolution=False, hooks=hooks
                    )
                except protocol.FrontendError:
                    raise e
                else:
                    raise Exception("Should be FrontendError but was: %s"
                                    % res)
            else:
                # The frontend test function were fine with the
                # interaction, notify backend that we're happy to go.
                driver.send(
                    protocol.RetryablePositive(retryable_id),
                    hooks=hooks
                )
        elif isinstance(res, protocol.RetryFunc):
            handle_retry_func(retry_func, res, driver)
        elif isinstance(res, protocol.RetryableDone):
            return x
        else:
            allowed = ["RetryableTry", "RetryableDone", "RetryFunc"]
            raise Exception("Should be one of %s but was: %s" % (allowed, res))
