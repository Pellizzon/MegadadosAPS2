import boto3
import pyspark
import math
import pandas as pd


def conta_documento(item):
    conteudo = item[1]
    palavras = conteudo.strip().split()
    palavras_ = [i for i in palavras if i.isalpha()]
    palavras_filtradas = [i for i in palavras_ if len(i) > 3]
    return [(i.lower(), 1) for i in set(palavras_filtradas)]


def calcula_idf(item):
    palavra, contagem = item
    idf = math.log10(N_docs / contagem)
    return (palavra, idf)


def filtra_doc(item):
    contagem = item[1]
    return (contagem < DOC_COUNT_MAX) and (contagem > DOC_COUNT_MIN)


def conta_palavra(item):
    url, conteudo = item
    palavras = conteudo.strip().split()
    palavras_ = [i for i in palavras if i.isalpha()]
    palavras_boas = []
    for i in range(len(palavras_)):
        if i < 5:
            if palavra1 in palavras_[: i + 5] or palavra2 in palavras_[: i + 5]:
                palavras_boas.append(palavras_[i])
        elif i > len(palavras_) - 5:
            if palavra1 in palavras_[i - 5 :] or palavra2 in palavras_[i - 5 :]:
                palavras_boas.append(palavras_[i])
        else:
            if (
                palavra1 in palavras_[i - 5 : i + 5]
                or palavra2 in palavras_[i - 5 : i + 5]
            ):
                palavras_boas.append(palavras_[i])

    palavras_filtradas = [i.lower() for i in palavras_boas if len(i) > 3]
    return [(i.lower(), 1) for i in palavras_filtradas]


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

    DOC_COUNT_MIN = 5
    DOC_COUNT_MAX = N_docs * 0.8

    rdd_idf = (
        rdd.flatMap(conta_documento)
        .reduceByKey(lambda x, y: x + y)
        .filter(filtra_doc)
        .map(lambda x: (x[0], math.log10(N_docs / x[1])))
    )

    palavra1 = "flamengo"
    palavra2 = "fluminense"

    rdd_freq_p1 = gera_rdd_freq(rdd, palavra1)
    rdd_freq_p2 = gera_rdd_freq(rdd, palavra2)

    rdd_freq_inter = rdd_freq_p1.intersection(rdd_freq_p2)

    rdd_freq_p1Only = rdd_freq_p1.subtractByKey(rdd_freq_inter)
    rdd_freq_p2Only = rdd_freq_p2.subtractByKey(rdd_freq_inter)

    rdd_relevancia = gera_relevancia(rdd_freq_inter, rdd_idf)
    rdd_relevanciaP1 = gera_relevancia(rdd_freq_p1Only, rdd_idf)
    rdd_relevanciaP2 = gera_relevancia(rdd_freq_p2Only, rdd_idf)

    top_relevancia = pega_top_100(rdd_relevancia)
    top_relevanciaP1 = pega_top_100(rdd_relevanciaP1)
    top_relevanciaP2 = pega_top_100(rdd_relevanciaP2)

    tops = [top_relevancia, top_relevanciaP1, top_relevanciaP2]
    # csv_names = ["top100_intersection.csv", f"top100_{palavra1}.csv", f"top100_{palavra2}.csv"]
    csv_names = [
        "brasil_top100_intersection.csv",
        f"brasil_top100_{palavra1}.csv",
        f"brasil_top100_{palavra2}.csv",
    ]

    for top, name in zip(tops, csv_names):
        df = pd.DataFrame(top, columns=["Palavra", "Relevancia"])
        df.to_csv(f"s3://megadados-alunos/matheus-pedro/{name}")
        # df.to_csv(f"{name}")

    sc.stop()
