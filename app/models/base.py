from abc import ABC, abstractmethod

class BaseModel(ABC):
    @abstractmethod
    def fit(self, merge_log_id: int):
        """
        Train the model using data associated with merge_log_id.
        Save the results to the database.
        """
        pass

    @abstractmethod
    def predict(self, email: str, limit: int = 5):
        """
        Return recommendations for the given email.
        """
        pass
