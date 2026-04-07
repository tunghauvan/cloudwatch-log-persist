"""
SQSConsumer: background daemon thread that long-polls an SQS queue for
S3 Event Notification messages and feeds them to ALBProcessor.

Flow:
  S3 (ALB log file written)
    → S3 Event Notification published to SQS
    → SQSConsumer.receive_message()  [long-poll, WaitTimeSeconds=20]
    → extract bucket + key from Records[*].s3
    → ALBProcessor.process_s3_object(bucket, key)
    → delete_message() on success  (re-delivery on error via visibility timeout)

Configuration keys (alb section in config.yaml):
  sqs_queue_url          "" = consumer disabled (default)
  sqs_poll_interval      seconds to sleep between receive_message calls (default 30)
  sqs_max_messages       messages per call, 1-10 (default 5)
  sqs_visibility_timeout seconds the message stays hidden while being processed (default 60)
"""

import json
import logging
import threading
import time
from typing import Any, Dict, Optional

import boto3

logger = logging.getLogger("service.sqs_consumer")


class SQSConsumer:
    """
    Background polling consumer.  Call start() once; call stop() on shutdown.
    """

    def __init__(self, warehouse, config: dict) -> None:
        self.warehouse = warehouse
        self.config = config

        alb_cfg = config.get("alb", {})
        self.queue_url: str = alb_cfg.get("sqs_queue_url", "").strip()
        self.poll_interval: int = int(alb_cfg.get("sqs_poll_interval", 30))
        self.max_messages: int = max(1, min(10, int(alb_cfg.get("sqs_max_messages", 5))))
        self.visibility_timeout: int = int(alb_cfg.get("sqs_visibility_timeout", 60))

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if not self.queue_url:
            logger.info("[SQS] sqs_queue_url is empty — consumer not started")
            return
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="sqs-consumer",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"[SQS] Consumer started: queue={self.queue_url} "
            f"max_messages={self.max_messages} poll_interval={self.poll_interval}s"
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("[SQS] Consumer stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_sqs_client(self):
        s3_cfg = self.config.get("s3", {})
        region = s3_cfg.get("region", "us-east-1")
        kwargs: Dict[str, Any] = {"region_name": region}
        if not s3_cfg.get("use_ec2_role", False):
            access_key = s3_cfg.get("access_key")
            secret_key = s3_cfg.get("secret_key")
            if access_key:
                kwargs["aws_access_key_id"] = access_key
            if secret_key:
                kwargs["aws_secret_access_key"] = secret_key
        return boto3.client("sqs", **kwargs)

    def _poll_loop(self) -> None:
        logger.info("[SQS] Poll loop starting")
        sqs = self._get_sqs_client()

        from service.services.alb_processor import ALBProcessor
        processor = ALBProcessor(self.warehouse, self.config)

        while not self._stop_event.is_set():
            try:
                self._receive_and_process(sqs, processor)
            except Exception as exc:
                logger.error(f"[SQS] Unexpected error in poll loop: {exc}", exc_info=True)

            # Sleep in small increments so stop() is responsive
            for _ in range(self.poll_interval):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

        logger.info("[SQS] Poll loop exited")

    def _receive_and_process(self, sqs, processor) -> None:
        response = sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.max_messages,
            WaitTimeSeconds=20,           # long poll
            VisibilityTimeout=self.visibility_timeout,
        )
        messages = response.get("Messages", [])
        if not messages:
            return

        logger.info(f"[SQS] Received {len(messages)} message(s)")

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            success = self._handle_message(msg, processor)
            if success:
                sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt)
                logger.debug(f"[SQS] Deleted message {msg['MessageId']}")
            else:
                # Leave message in queue; it re-appears after VisibilityTimeout
                logger.warning(
                    f"[SQS] Message {msg['MessageId']} not deleted — "
                    "will be re-delivered after visibility timeout"
                )

    def _handle_message(self, msg: dict, processor) -> bool:
        """
        Extract S3 Event records and invoke ALBProcessor for each object.
        Returns True only when every record processed successfully.
        """
        try:
            body = json.loads(msg.get("Body", "{}"))
        except json.JSONDecodeError as exc:
            logger.error(f"[SQS] Cannot parse message body: {exc}")
            return False

        # S3 Event Notification may be wrapped by SNS (TopicArn present)
        if "Message" in body:
            try:
                body = json.loads(body["Message"])
            except json.JSONDecodeError:
                logger.error("[SQS] Cannot parse nested SNS Message field")
                return False

        records = body.get("Records", [])
        if not records:
            # Test notification or unknown format — ack and ignore
            logger.debug(f"[SQS] No Records in message {msg['MessageId']}, ignoring")
            return True

        all_ok = True
        for rec in records:
            event_source = rec.get("eventSource", "")
            if event_source and event_source != "aws:s3":
                logger.debug(f"[SQS] Skipping non-S3 record: {event_source}")
                continue

            s3_info = rec.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name")
            key = s3_info.get("object", {}).get("key")

            if not bucket or not key:
                logger.warning(f"[SQS] Record missing bucket/key: {rec}")
                all_ok = False
                continue

            result = processor.process_s3_object(bucket, key)
            if result.get("status") == "error":
                logger.error(
                    f"[SQS] Processing failed for s3://{bucket}/{key}: "
                    f"{result.get('error')}"
                )
                all_ok = False
            else:
                logger.info(
                    f"[SQS] Processed s3://{bucket}/{key}: "
                    f"{result.get('rows_written', 0)} rows in "
                    f"{result.get('duration_ms', 0)}ms"
                )

        return all_ok
