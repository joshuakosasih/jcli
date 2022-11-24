import yaml
import codecs
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
from elasticsearch_dsl import Search, Index, UpdateByQuery


class Profile:
    def __init__(self, name: str = None):
        if name is not None:
            with codecs.open(f"{name}.pf", encoding='hex') as f:
                self.from_dict(yaml.safe_load(f.read()))
        else:
            self.__prompt()

    def __prompt(self):
        self.hosts = eval(input("Enter hosts (list): "))
        self.username = input("Enter username: ")
        self.password = input("Enter password: ")

    def from_dict(self, d: dict):
        self.hosts = d["hosts"]
        self.username = d["user"]
        self.password = d["pass"]

    def to_dict(self) -> dict:
        return {"hosts": self.hosts, "user": self.username, "pass": self.password}

    def __str__(self) -> str:
        return str(self.to_dict())

    def save(self, name: str):
        with codecs.open(f"{name}.pf", 'wb', encoding='hex') as f:
            f.write(bytes(yaml.safe_dump(self.to_dict()), 'utf-8'))


class Jes:
    def __init__(self, cli, index="", size=10):
        self.client = cli
        self.idx_name = index
        self.size = size

    def to_dict(self) -> dict:
        return {"cli": self.client, "idx": self.idx_name, "size": self.size}

    def __str__(self) -> str:
        return str(self.to_dict())

    def index(self) -> Index:
        return Index(self.idx_name, using=self.client)

    def refresh(self):
        self.index().refresh()

    def search(self, select: [str] = None) -> Search:
        if select is None:
            select = []
        return Search(using=self.client, index=self.idx_name).extra(size=self.size).source(select)

    def match(self, select: [str] = None, **where) -> Search:
        return self.search(select).query("match", **where)

    def regexp(self, select: [str] = None, **where) -> Search:
        return self.search(select).query("regexp", **where)

    def update(self, update_query: str, **where) -> UpdateByQuery:
        return UpdateByQuery(using=self.client, index=self.idx_name).query("match", **where)\
            .script(source=f"ctx._source.{update_query}", lang="painless")

    def xmatch(self, select: [str] = None, **where):
        return self.match(select, **where).execute()

    def xregexp(self, select: [str] = None, **where):
        return self.regexp(select, **where).execute()

    def xupdate(self, update_query: str, **where):
        return self.update(update_query, **where).execute()


def get_es_cli(profile_name: str = None):
    p = Profile(profile_name)
    return Elasticsearch(hosts=p.hosts, http_auth=(p.username, p.password), verify_certs=False)


def get_os_cli(profile_name: str = None):
    p = Profile(profile_name)
    return OpenSearch(hosts=p.hosts, http_auth=(p.username, p.password), verify_certs=False)
