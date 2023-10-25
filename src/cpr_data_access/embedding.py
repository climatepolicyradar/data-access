from typing import List, Literal

import numpy as np
from sentence_transformers import SentenceTransformer

ModelName = Literal["msmarco-distilbert-dot-v5"]


class Embedder:
    """Class for embedding strings using a sentence-transformers model"""

    def __init__(self, model_name: ModelName = "msmarco-distilbert-dot-v5"):
        self.model = SentenceTransformer(model_name)

    def embed(
        self,
        string: str,
        normalize: bool = True,
    ) -> List[float]:
        """
        Embed a string using a sentence-transformers model

        :param string: the string to embed
        :param normalize: whether to normalize the embedding
        """
        embedding = self.model.encode(string, convert_to_numpy=True)
        if normalize:
            embedding = embedding / np.linalg.norm(embedding, keepdims=True)

        return embedding.tolist()
