import os
from haystack import Finder
from haystack.indexing.cleaning import clean_wiki_text
from haystack.indexing.utils import convert_files_to_dicts, fetch_archive_from_http
from haystack.reader.farm import FARMReader
from haystack.reader.transformers import TransformersReader
from haystack.utils import print_answers
from haystack.database.elasticsearch import ElasticsearchDocumentStore
from haystack.retriever.sparse import ElasticsearchRetriever

class QNA:

    def __init__(self, db, s3, orcomm):
        self.db = db
        self.s3 = s3
        self.orcomm = orcomm
        self.timer = None
        self.intervalIsActive = False
        self.taskIsActive = False
        #self.startTimer()
        self.documentStore = self.connectElasticSearch()
        #self.injectSampleData(self.documentStore)
        retriever = self.setRetriever(self.documentStore)
        reader = self.setReader()
        self.setPrediction(reader, retriever)

    def connectElasticSearch(self):
        return ElasticsearchDocumentStore(host='elasticsearch', username='', password='', index='document')

    def injectSampleData(self, documentStore):
        # Let's first get some documents that we want to query
        # Here: 517 Wikipedia articles for Game of Thrones
        doc_dir = 'data/article_txt_got'
        s3_url = 'https://s3.eu-central-1.amazonaws.com/deepset.ai-farm-qa/datasets/documents/wiki_gameofthrones_txt.zip'
        fetch_archive_from_http(url=s3_url, output_dir=doc_dir)
        # Convert files to dicts
        # You can optionally supply a cleaning function that is applied to each doc (e.g. to remove footers)
        # It must take a str as input, and return a str.
        dicts = convert_files_to_dicts(dir_path=doc_dir, clean_func=clean_wiki_text, split_paragraphs=True)
        # We now have a list of dictionaries that we can write to our document store.
        # If your texts come from a different source (e.g. a DB), you can of course skip convert_files_to_dicts() and create the dictionaries yourself.
        # The default format here is: {"name": "<some-document-name>, "text": "<the-actual-text>"}
        # (Optionally: you can also add more key-value-pairs here, that will be indexed as fields in Elasticsearch and
        # can be accessed later for filtering or shown in the responses of the Finder)

        # Let's have a look at the first 3 entries:
        print(dicts[:3])

        # Now, let's write the dicts containing documents to our DB.
        documentStore.write_documents(dicts)

    def setRetriever(self, documentStore):
        return ElasticsearchRetriever(document_store=documentStore)

    def setReader(self):
        return FARMReader(model_name_or_path="deepset/roberta-base-squad2", use_gpu=False)
        # Alternative:
        # reader = TransformersReader(model="distilbert-base-uncased-distilled-squad", tokenizer="distilbert-base-uncased", use_gpu=-1)

    def setPrediction(self, reader, retriever):
        finder = Finder(reader, retriever)
        # You can configure how many candidates the reader and retriever shall return
        # The higher top_k_retriever, the better (but also the slower) your answers. 
        prediction = finder.get_answers(question="Who is the father of Arya Stark?", top_k_retriever=10, top_k_reader=5)
        # prediction = finder.get_answers(question="Who created the Dothraki vocabulary?", top_k_reader=5)
        # prediction = finder.get_answers(question="Who is the sister of Sansa?", top_k_reader=5)
        print_answers(prediction, details="minimal")