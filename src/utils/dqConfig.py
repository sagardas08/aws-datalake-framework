class Config:
    __conf = {
        "min": ".hasMin",
        "null": ".isComplete",
        "pk": ".isUnique",
        "data_type": ".hasDataType",
        "max_length": ".hasMaxLength"
    }

    @staticmethod
    def config(check):
        return Config.__conf[check]