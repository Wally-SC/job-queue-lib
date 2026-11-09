import threading

import pytest

from jobqueue import FilaDeTarefas, StatusJob


def test_executa_e_devolve_resultado():
    with FilaDeTarefas(workers=2) as fila:
        job = fila.submeter(lambda a, b=0: a + b, 2, b=3)
        assert job.esperar(timeout=2) == 5
        assert job.status is StatusJob.CONCLUIDO
        assert job.tentativas == 1


def test_prioridade_e_fifo_no_empate():
    ordem = []
    liberar = threading.Event()
    fila = FilaDeTarefas(workers=1)
    try:
        # ocupa o único worker para garantir que os demais fiquem enfileirados
        bloqueio = fila.submeter(liberar.wait, 5)
        jobs = [
            fila.submeter(ordem.append, "baixa", prioridade=0),
            fila.submeter(ordem.append, "alta-1", prioridade=10),
            fila.submeter(ordem.append, "media", prioridade=5),
            fila.submeter(ordem.append, "alta-2", prioridade=10),
        ]
        liberar.set()
        bloqueio.esperar(timeout=2)
        for j in jobs:
            j.esperar(timeout=2)
    finally:
        fila.encerrar()
    assert ordem == ["alta-1", "alta-2", "media", "baixa"]


def test_retentativas_com_backoff_exponencial():
    esperas = []
    chamadas = {"n": 0}

    def instavel():
        chamadas["n"] += 1
        if chamadas["n"] < 3:
            raise ConnectionError("falha temporária")
        return "ok"

    with FilaDeTarefas(workers=1, backoff_base=1.0, sleep=esperas.append) as fila:
        job = fila.submeter(instavel, max_tentativas=5)
        assert job.esperar(timeout=2) == "ok"

    assert job.tentativas == 3
    assert esperas == [1.0, 2.0]  # 1s e 2s: dobra a cada falha


def test_falha_definitiva_relanca_ultimo_erro():
    esperas = []

    def sempre_falha():
        raise ValueError("quebrou")

    with FilaDeTarefas(workers=1, backoff_base=0.5, sleep=esperas.append) as fila:
        job = fila.submeter(sempre_falha, max_tentativas=3)
        with pytest.raises(ValueError, match="quebrou"):
            job.esperar(timeout=2)

    assert job.status is StatusJob.FALHOU
    assert job.tentativas == 3
    assert esperas == [0.5, 1.0]  # sem espera depois da última tentativa


def test_sem_retentativa_por_padrao():
    with FilaDeTarefas(workers=1) as fila:
        job = fila.submeter(lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            job.esperar(timeout=2)
    assert job.tentativas == 1


def test_uma_falha_nao_derruba_o_worker():
    with FilaDeTarefas(workers=1) as fila:
        ruim = fila.submeter(lambda: 1 / 0)
        bom = fila.submeter(lambda: "vivo")
        assert bom.esperar(timeout=2) == "vivo"
        with pytest.raises(ZeroDivisionError):
            ruim.esperar(timeout=2)


def test_timeout_na_espera():
    liberar = threading.Event()
    fila = FilaDeTarefas(workers=1)
    try:
        job = fila.submeter(liberar.wait, 5)
        with pytest.raises(TimeoutError):
            job.esperar(timeout=0.05)
    finally:
        liberar.set()
        fila.encerrar()


def test_encerrar_termina_o_que_ja_estava_na_fila():
    resultados = []
    fila = FilaDeTarefas(workers=2)
    jobs = [fila.submeter(resultados.append, i) for i in range(20)]
    fila.encerrar()  # aguarda por padrão
    assert sorted(resultados) == list(range(20))
    assert all(j.status is StatusJob.CONCLUIDO for j in jobs)


def test_submeter_apos_encerrar_falha():
    fila = FilaDeTarefas(workers=1)
    fila.encerrar()
    with pytest.raises(RuntimeError):
        fila.submeter(print)


def test_muitos_jobs_concorrentes():
    with FilaDeTarefas(workers=4) as fila:
        jobs = [fila.submeter(lambda n=n: n * n) for n in range(200)]
        assert sum(j.esperar(timeout=5) for j in jobs) == sum(n * n for n in range(200))
        assert fila.estatisticas()["concluido"] == 200


def test_argumentos_invalidos():
    with pytest.raises(ValueError):
        FilaDeTarefas(workers=0)
    with FilaDeTarefas(workers=1) as fila:
        with pytest.raises(ValueError):
            fila.submeter(print, max_tentativas=0)
