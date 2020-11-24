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


def conta_palavra_global(item):
    url, conteudo = item
    palavras = conteudo.strip().split()
    palavras_ = [i for i in palavras if i.isalpha()]
    palavras_filtradas = [i.lower() for i in palavras_ if len(i) > 3]
    return [(i.lower(), 1) for i in palavras_filtradas]


def conta_palavra_local(item):
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


def gera_rdd_freq_global(rdd):
    rdd_freq = (
        rdd.flatMap(conta_palavra_global)
        .reduceByKey(lambda x, y: x + y)
        .map(calcula_freq)
    )
    return rdd_freq


def gera_rdd_freq_local(rdd):
    rdd_freq = (
        rdd.flatMap(conta_palavra_local)
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

    rdd_p1 = rdd.filter(lambda x: palavra1 in x[1])
    rdd_p2 = rdd.filter(lambda x: palavra2 in x[1])

    rdd_freq_p1_global = gera_rdd_freq_global(rdd_p1)
    rdd_freq_p2_global = gera_rdd_freq_global(rdd_p2)

    rdd_freq_inter_global = rdd_freq_p1_global.intersection(rdd_freq_p2_global)

    rdd_freq_p1Only_global = rdd_freq_p1_global.subtractByKey(rdd_freq_inter_global)
    rdd_freq_p2Only_global = rdd_freq_p2_global.subtractByKey(rdd_freq_inter_global)

    rdd_relevancia_global = gera_relevancia(rdd_freq_inter_global, rdd_idf)
    rdd_relevanciaP1_global = gera_relevancia(rdd_freq_p1Only_global, rdd_idf)
    rdd_relevanciaP2_global = gera_relevancia(rdd_freq_p2Only_global, rdd_idf)

    top_relevancia_global = pega_top_100(rdd_relevancia_global)
    top_relevanciaP1_global = pega_top_100(rdd_relevanciaP1_global)
    top_relevanciaP2_global = pega_top_100(rdd_relevanciaP2_global)

    tops_global = [
        top_relevancia_global,
        top_relevanciaP1_global,
        top_relevanciaP2_global,
    ]

    csv_names_global = [
        "brasil_top100_intersection_global.csv",
        f"brasil_top100_{palavra1}_global.csv",
        f"brasil_top100_{palavra2}_global.csv",
    ]

    for top, name in zip(tops_global, csv_names_global):
        df = pd.DataFrame(top, columns=["Palavra", "Relevancia"])
        df.to_csv(f"s3://megadados-alunos/matheus-pedro/{name}")
        # df.to_csv(f"{name}")

    rdd_freq_p1_local = gera_rdd_freq_local(rdd_p1)
    rdd_freq_p2_local = gera_rdd_freq_local(rdd_p2)

    rdd_freq_inter_local = rdd_freq_p1_local.intersection(rdd_freq_p2_local)

    rdd_freq_p1Only_local = rdd_freq_p1_local.subtractByKey(rdd_freq_inter_local)
    rdd_freq_p2Only_local = rdd_freq_p2_local.subtractByKey(rdd_freq_inter_local)

    rdd_relevancia_local = gera_relevancia(rdd_freq_inter_local, rdd_idf)
    rdd_relevanciaP1_local = gera_relevancia(rdd_freq_p1Only_local, rdd_idf)
    rdd_relevanciaP2_local = gera_relevancia(rdd_freq_p2Only_local, rdd_idf)

    top_relevancia_local = pega_top_100(rdd_relevancia_local)
    top_relevanciaP1_local = pega_top_100(rdd_relevanciaP1_local)
    top_relevanciaP2_local = pega_top_100(rdd_relevanciaP2_local)

    tops_local = [top_relevancia_local, top_relevanciaP1_local, top_relevanciaP2_local]

    csv_names_local = [
        "brasil_top100_intersection_local.csv",
        f"brasil_top100_{palavra1}_local.csv",
        f"brasil_top100_{palavra2}_local.csv",
    ]

    for top, name in zip(tops_local, csv_names_local):
        df = pd.DataFrame(top, columns=["Palavra", "Relevancia"])
        df.to_csv(f"s3://megadados-alunos/matheus-pedro/{name}")
        # df.to_csv(f"{name}")

    sc.stop()
