import yaml
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
from elasticsearch_dsl import Search, Index


class Profile:
    def __init__(self, name=None):
        if name is not None:
            with open(f"{name}.pf", encoding='utf-8') as f:
                self.from_dict(yaml.safe_load(f.read()))
        else:
            self.__prompt()

    def __prompt(self):
        self.hosts = eval(input("Enter hosts (list): "))
        self.username = input("Enter username (str): ")
        self.password = input("Enter password (str): ")

    def from_dict(self, d):
        self.hosts = d["hosts"]
        self.username = d["user"]
        self.password = d["pass"]

    def to_dict(self):
        return {"hosts": self.hosts, "user": self.username, "pass": self.password}

    def save(self, name):
        with open(f"{name}.pf", 'w', encoding='utf-8') as f:
            f.write(yaml.safe_dump(self.to_dict()))


class Jes:
    def __init__(self, cli=None):
        self.client = cli
        self.idx_name = ""
        self.size = 10

    def index(self):
        return Index(self.idx_name, using=self.client)

    def search(self, select=None):
        if select is None:
            select = []
        return Search(using=self.client, index=self.idx_name).extra(size=self.size).source(select)

    def match(self, select=None, **kwargs):
        return self.search(select).query("match", **kwargs)

    def regexp(self, select=None, **kwargs):
        return self.search(select).query("regexp", **kwargs)

    def xmatch(self, select=None, **kwargs):
        return self.match(select, **kwargs).execute()

    def xregexp(self, select=None, **kwargs):
        return self.regexp(select, **kwargs).execute()


def get_es_cli(profile_name=None):
    p = Profile(profile_name)
    return Elasticsearch(hosts=p.hosts, http_auth=(p.username, p.password), verify_certs=False)


def get_os_cli(profile_name=None):
    p = Profile(profile_name)
    return OpenSearch(hosts=p.hosts, http_auth=(p.username, p.password), verify_certs=False)
