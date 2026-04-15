import sys

from app.pipeline.etl_sales_v2 import executar_etl


def main():
	try:
		summary = executar_etl()
	except RuntimeError as exc:
		print(str(exc))
		return 1

	print("Resumo ETL")
	print("Arquivos encontrados:", summary["files_found"])
	print("Arquivos carregados:", summary["files_loaded"])
	print("Arquivos inválidos:", summary["files_invalid"])
	print("Arquivos já processados:", summary["files_skipped"])
	print("Arquivos com falha:", summary["files_failed"])
	print("Linhas carregadas:", summary["rows_loaded"])
	return 0


if __name__ == "__main__":
	raise SystemExit(main())