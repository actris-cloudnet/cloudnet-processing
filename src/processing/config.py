import os


class Config:
    def __init__(self, environ=os.environ) -> None:
        self.storage_service_url = environ["STORAGE_SERVICE_URL"].rstrip("/")
        self.storage_service_auth = (
            environ["STORAGE_SERVICE_USER"],
            environ["STORAGE_SERVICE_PASSWORD"],
        )
        self.dataportal_public_url = environ["DATAPORTAL_PUBLIC_URL"].rstrip("/")
        self.dataportal_url = environ["DATAPORTAL_URL"].rstrip("/")
        self.data_submission_auth = (
            environ.get("DATA_SUBMISSION_USERNAME", "admin"),
            environ.get("DATA_SUBMISSION_PASSWORD", "admin"),
        )
        self.pid_service_url = environ["PID_SERVICE_URL"].rstrip("/")
        self.is_production = environ["PID_SERVICE_TEST_ENV"].lower() != "true"
        self.freeze_after_days = int(environ["FREEZE_AFTER_DAYS"])
        self.freeze_model_after_days = int(environ["FREEZE_MODEL_AFTER_DAYS"])
        self.slack_api_token = environ.get("SLACK_API_TOKEN")
        self.slack_channel_id = environ.get("SLACK_CHANNEL_ID")
        self.dvas_portal_url = environ["DVAS_PORTAL_URL"].rstrip("/")
        self.dvas_access_token = environ["DVAS_ACCESS_TOKEN"]
        self.dvas_username = environ["DVAS_USERNAME"]
        self.dvas_password = environ["DVAS_PASSWORD"]
        self.dvas_provider_id = "11"  # CLU
