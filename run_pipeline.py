"""
Script orquestrador do pipeline de dados.
Executa: download → limpeza → integração → transformação.

Uso:
    python run_pipeline.py              # Pipeline completo
    python run_pipeline.py --skip-download  # Pular download (usar dados existentes)
"""

import sys
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def main():
    skip_download = "--skip-download" in sys.argv
    start = time.time()

    print("=" * 60)
    print("  ANVISA VigiMed — Pipeline de Dados")
    print("=" * 60)

    # 1. Download
    if not skip_download:
        print("\n[1/4] Baixando datasets da ANVISA...")
        from src.collector.download import download_all
        download_all()
    else:
        print("\n[1/4] Download pulado (--skip-download)")

    # 2. Limpeza
    print("\n[2/4] Limpando e tratando dados...")
    from src.pipeline.clean import clean_all
    clean_all()

    # 3. Integração
    print("\n[3/4] Integrando datasets...")
    from src.pipeline.integrate import integrate_all
    integrate_all()

    # 4. Transformação
    print("\n[4/4] Criando variaveis derivadas e agregacoes...")
    from src.pipeline.transform import transform_all
    transform_all()

    elapsed = time.time() - start
    print("\n" + "=" * 60)
    print(f"  Pipeline concluido em {elapsed:.0f}s")
    print("  Dados em: data/processed/")
    print("  Iniciar dashboard: python -m src.dashboard.app")
    print("=" * 60)


if __name__ == "__main__":
    main()
