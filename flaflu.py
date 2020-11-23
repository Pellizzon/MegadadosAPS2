import boto3
import pyspark
import math
import pandas as pd


def limpa_conteudo(conteudo):
    to_remove = [
        "!",
        ".",
        ",",
        ":",
        "@",
        "#",
        "$",
        "%",
        "/",
        "\\",
        "|",
        "´",
        "`",
        "*",
        "&",
        "(",
        ")",
        "[",
        "]",
        "}",
        "{",
        "+",
        "-",
        "<",
        ">",
        "?",
        "°",
        "=",
        '"',
        "_",
        "'",
        ";",
        "^",
        "~",
        "¨",
    ]
    for i in to_remove:
        conteudo = conteudo.replace(i, " ")
    return conteudo


def conta_documento(item):
    conteudo = limpa_conteudo(item[1])
    palavras = conteudo.strip().split()
    return [(i.lower(), 1) for i in set(palavras)]


def calcula_idf(item):
    palavra, contagem = item
    idf = math.log10(N_docs / contagem)
    return (palavra, idf)


def filtra_doc(item):
    contagem = item[1]
    return (contagem < DOC_COUNT_MAX) and (contagem > DOC_COUNT_MIN)


def conta_palavra(item):
    conteudo = limpa_conteudo(item[1])
    palavras = conteudo.strip().split()
    return [(i.lower(), 1) for i in palavras]


def calcula_freq(item):
    palavra, contagem = item
    freq = math.log10(1 + contagem)
    return (palavra, freq)


def gera_rdd_freq(rdd, palavra):
    rdd_freq = (
        rdd.filter(lambda x: palavra in x[1])
        .flatMap(conta_palavra)
        .reduceByKey(lambda x, y: x + y)
        .map(calcula_freq)
    )

    return rdd_freq


def gera_relevancia(rdd_freq, rdd_idf):
    relevancia = rdd_freq.join(rdd_idf).map(lambda x: (x[0], x[1][0] * x[1][1]))
    return relevancia


def pega_top_100(rdd):
    return rdd.takeOrdered(100, key=lambda x: -x[1])


if __name__ == "__main__":

    sc = pyspark.SparkContext(appName="flaflu")
    rdd = sc.sequenceFile("s3://megadados-alunos/web-brasil")
    # rdd = sc.sequenceFile("part-00000")
    N_docs = rdd.count()

    DOC_COUNT_MIN = 10
    DOC_COUNT_MAX = N_docs * 0.7

    rdd_idf = (
        rdd.flatMap(conta_documento)
        .reduceByKey(lambda x, y: x + y)
        .filter(filtra_doc)
        .map(lambda x: (x[0], math.log10(N_docs / x[1])))
    )

    rdd_freq_fla = gera_rdd_freq(rdd, "flamengo")
    rdd_freq_flu = gera_rdd_freq(rdd, "fluminense")

    rdd_freq_flaflu = rdd_freq_fla.intersection(rdd_freq_flu)

    rdd_freq_flaOnly = rdd_freq_fla.subtractByKey(rdd_freq_flaflu)
    rdd_freq_fluOnly = rdd_freq_flu.subtractByKey(rdd_freq_flaflu)

    rdd_relevancia = gera_relevancia(rdd_freq_flaflu, rdd_idf)
    rdd_relevanciaFLA = gera_relevancia(rdd_freq_flaOnly, rdd_idf)
    rdd_relevanciaFLU = gera_relevancia(rdd_freq_fluOnly, rdd_idf)

    top_relevancia = pega_top_100(rdd_relevancia)
    top_relevanciaFLA = pega_top_100(rdd_relevanciaFLA)
    top_relevanciaFLU = pega_top_100(rdd_relevanciaFLU)

    tops = [top_relevancia, top_relevanciaFLA, top_relevanciaFLU]
    csv_names = ["top100_intersection.csv", "top100_FLA.csv", "top100_FLU.csv"]

    for top, name in zip(tops, csv_names):
        df = pd.DataFrame(top, columns=["Palavra", "Relevancia"])
        df.to_csv(f"s3://megadados-alunos/matheus-pedro/{name}")
        # df.to_csv(f"{name}")

    sc.stop()
