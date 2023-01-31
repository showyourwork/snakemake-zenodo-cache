from zenodo_cache import ZenodoCache

cache = ZenodoCache("cache")

rule all:
    input:
        cache.from_cache("plot.png")

rule data:
    output:
        "data.json"
    shell:
        "curl -L -o {output} https://raw.githubusercontent.com/dfm/cv/main/data/pubs.json"

rule plot:
    input:
        "data.json"
    output:
        cache.to_cache("plot.png")
    shell:
        "touch {output}"
