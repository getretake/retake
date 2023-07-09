import pinecone

from abc import ABC, abstractmethod
from core.load.base import Loader
from typing import Dict, List, Union, Optional, Any, cast


class PineconeLoader(Loader):
    def __init__(
        self,
        api_key: str,
        environment: str,
    ) -> None:
        pinecone.init(api_key=api_key, environment=environment)

    def _check_index_exists(self, index_name: str) -> bool:
        try:
            pinecone.describe_index(index_name)
            return True
        except pinecone.NotFoundException as e:
            return False

    def _get_num_dimensions(self, index_name: str) -> int:
        return int(pinecone.describe_index(index_name).dimension)

    def _create_index(self, index_name: str, num_dimensions: int) -> None:
        pinecone.create_index(index_name, dimension=num_dimensions)

    def check_and_setup_index(self, index_name: str, num_dimensions: int) -> None:
        if not self._check_index_exists(index_name=index_name):
            self._create_index(index_name=index_name, num_dimensions=num_dimensions)
        else:
            if self._get_num_dimensions(index_name=index_name) != num_dimensions:
                raise ValueError(
                    f"Index {index_name} already exists with a different dimension"
                )

    ### Public Methods ###
    def upsert_embedding(
        self,
        index_name: str,
        namespace: str,
        embedding: List[float],
        id: Union[str, int],
        metadata: Optional[Dict[str, Any]],
    ) -> None:
        index = pinecone.Index(name=index_name)

        doc = {"id": id, "values": embedding}

        if not metadata is None:
            doc["metadata"] = metadata

        index.upsert(vectors=[doc], namespace=namespace)

    def bulk_upsert_embeddings(
        self,
        index_name: str,
        namespace: str,
        embeddings: List[List[float]],
        ids: List[Union[str, int]],
        metadata: Optional[List[Dict[str, Any]]],
    ) -> None:
        num_dimensions = len(embeddings[0])
        num_embeddings = len(embeddings)
        docs = []

        if not all(len(embedding) == num_dimensions for embedding in embeddings):
            raise ValueError("Not all embeddings have the same number of dimensions")

        if not len(ids) == num_embeddings:
            raise ValueError("Number of ids does not match number of embeddings")

        if metadata is not None:
            docs = [
                {"id": doc_id, "values": embedding, "metadata": meta}
                for doc_id, embedding, meta in zip(ids, embeddings, metadata)
            ]
        else:
            docs = [
                {"id": doc_id, "values": embedding}
                for doc_id, embedding in zip(ids, embeddings)
            ]

        index = pinecone.Index(name=index_name)
        index.upsert(vectors=docs, namespace=namespace)
