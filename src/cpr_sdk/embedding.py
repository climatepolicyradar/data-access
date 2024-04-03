from typing import List, Literal, Optional

import numpy as np
from sentence_transformers import SentenceTransformer

ModelName = Literal["msmarco-distilbert-dot-v5"]


class Embedder:
    """Class for embedding strings using a sentence-transformers model"""

    def __init__(
        self,
        model_name: ModelName = "msmarco-distilbert-dot-v5",
        cache_folder: Optional[str] = None,
    ):
        self.model = SentenceTransformer(model_name, cache_folder=cache_folder)

    def embed(
        self,
        string: str,
        normalize: bool = False,
        show_progress_bar: bool = True,
    ) -> List[float]:
        """
        Embed a string using the configured sentence-transformers model

        :param string: the string to embed
        :param normalize: whether to normalize the embedding
        """
        embedding = self.model.encode(
            string,
            convert_to_numpy=True,
            show_progress_bar=show_progress_bar,
        )
        if normalize:
            embedding = embedding / np.linalg.norm(embedding, keepdims=True)

        return embedding.tolist()
