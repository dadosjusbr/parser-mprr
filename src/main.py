import sys
import os
import metadata
import data

from coleta import coleta_pb2 as Coleta, IDColeta
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf import text_format
from parser import parse

if "YEAR" in os.environ:
  YEAR = os.environ["YEAR"]
else:
  sys.stderr.write("YEAR environment variable not set\n")
  os._exit(1)

if "MONTH" in os.environ:
  MONTH = os.environ["MONTH"]
else:
  sys.stderr.write("MONTH environment variable not set\n")
  os._exit(1)

if "OUTPUT_FOLDER" in os.environ:
  OUTPUT_FOLDER = os.environ["OUTPUT_FOLDER"]
else:
  OUTPUT_FOLDER = "/output"

if "GIT_COMMIT" in os.environ:
  CRAWLER_VERSION = os.environ["GIT_COMMIT"]
else:
  CRAWLER_VERSION = "unspecified"

def parse_execution(data, file_names):
  # Cria objeto com dados da coleta.
    coleta = Coleta.Coleta()
    coleta.chave_coleta = IDColeta("mprr", MONTH, YEAR)
    coleta.orgao = "mprr"
    coleta.mes = int(MONTH)
    coleta.ano = int(YEAR)
    coleta.repositorio_coletor = "https://github.com/dadosjusbr/coletor-mprr"
    coleta.versao_coletor = CRAWLER_VERSION
    coleta.arquivos.extend(file_names)
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    coleta.timestamp_coleta.CopyFrom(timestamp)

    # Consolida folha de pagamento
    leaf = Coleta.FolhaDePagamento()
    leaf = parse(data, coleta.chave_coleta, MONTH, YEAR)

    # Monta resultado da coleta.
    rc = Coleta.ResultadoColeta()
    rc.folha.CopyFrom(leaf)
    rc.coleta.CopyFrom(coleta)

    metadatas = metadata.catch(int(MONTH), int(YEAR))
    rc.metadados.CopyFrom(metadatas)

    # Imprime a versão textual na saída padrão.
    print(text_format.MessageToString(rc), flush=True, end="")
  
#Main execution
def main():
  file_names = [f.rstrip() for f in sys.stdin.readlines()]
  datas = data.load(file_names,YEAR,MONTH,OUTPUT_FOLDER)
  datas.validate()
  parse_execution(datas,file_names)
  
if __name__ == "__main__":
  main()
  
