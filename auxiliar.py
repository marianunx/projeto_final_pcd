# Arquivo auxiliar.py em que eu coloquei todas as funções de calculo

import pandas as pd

def meta1(df):
    result = (
    df["julgados_2025"].sum() / (df["casos_novos_2025"].sum()
    + df["dessobrestados_2025"].sum() - df["suspensos_2025"].sum())) * 100
    
    return result


def meta2A(df):
    result = (
    df["julgm2_a"].sum() / (df["distm2_a"].sum() - df["suspm2_a"].sum())) * (1000 / 8)
    return result
    

def meta2B(df):
    result = (
    df["julgm2_b"].sum() / (df["distm2_b"].sum() - df["suspm2_b"].sum())) * (1000 / 9)

    return result


def meta2C(df):
    result = (
    df["julgm2_c"].sum() / (df["distm2_c"].sum() - df["suspm2_c"].sum())) * (1000 / 9.5)

 
    return result


def meta2ANT(df):
    result = (
    df["julgm2_ant"].sum() / (df["distm2_ant"].sum() - df["suspm2_ant"].sum())) * 100

    return result


def meta4A(df):
    result = (
    df["julgm4_a"].sum() / (df["distm4_a"].sum() - df["suspm4_a"].sum())) * (1000 / 6.5)

    return result


def meta4B(df):
    result = (
    df["julgm4_b"].sum() / (df["distm4_b"].sum() - df["suspm4_b"].sum())) * 100

    return result


def meta6(df):
    result = (
    df["julgm6_a"].sum() / (df["distm6_a"].sum() - df["suspm6_a"].sum())) * 100

    return result


def meta7A(df):
    result = (
    df["julgm7_a"].sum() / (df["distm7_a"].sum() - df["suspm7_a"].sum())) * 100

    return result


def meta7B(df):
    result = (
    df["julgm7_b"].sum() / (df["distm7_b"].sum() - df["suspm7_b"].sum())) * (1000 / 5)

    return result


def meta8A(df):
    result = (
    df["julgm8_a"].sum() / (df["distm8_a"].sum() - df["suspm8_a"].sum())) * (1000 / 7.5)

    return result



def meta8B(df):
    result = (
    df["julgm8_b"].sum() / (df["distm8_b"].sum() - df["suspm8_b"].sum())) * (1000 / 9)

    return result


def meta10A(df):
    result = (
    df["julgm10_a"].sum() / (df["distm10_a"].sum() - df["suspm10_a"].sum())) * (1000 / 9)

    return result


def meta10B(df):
    result = (
    df["julgm10_b"].sum() / (df["distm10_b"].sum() - df["suspm10_b"].sum())) * (1000 / 10)

    return result