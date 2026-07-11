"""
Módulo de Simulação de Transações de Vendas para o Azure Event Hubs.
Este script lê o dataset real 'Sales Transaction v.4a.csv' e envia as linhas
como eventos JSON em tempo real imitando o comportamento do microsserviço de pedidos.
"""
import os
import csv
import json
import logging
import sys

from dotenv import load_dotenv
from typing import Dict, Any
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError

# Carrega as variáveis de ambiente do arquivo `.env`
load_dotenv() 

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("OrdersMicroservice")

# Configurações por Variáveis de Ambiente
EVENT_HUB_CONNECTION_STR = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")


def parse_and_format_row(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Realiza o parse das colunas do CSV para tipagens apropriadas de microsserviços,
    tratando possíveis valores nulos do dataset original.
    """
    try:
        return {
            "TransactionNo": row.get("TransactionNo"),
            "Date": row.get("Date"),
            "ProductNo": row.get("ProductNo"),
            "ProductName": row.get("ProductName"),
            "Price": float(row["Price"]) if row.get("Price") else 0.0,
            "Quantity": int(row["Quantity"]) if row.get("Quantity") else 0,
            "CustomerNo": int(float(row["CustomerNo"])) if row.get("CustomerNo") else None, # Dataset possui decimais
            "Country": row.get("Country")
        }
    except ValueError as ve:
        logger.warning(f"Falha na conversão de tipos para a linha {row.get('TransactionNo')}: {ve}")
        # Retorna a estrutura com os dados brutos como fallback seguro
        return dict(row)


def stream_transactions_from_csv(file_path: str, client: EventHubProducerClient) -> None:
    """
    Abre o arquivo CSV de transações e envia linha por linha simulando eventos contínuos.
    """
    if not os.path.exists(file_path):
        logger.error(f"Arquivo de dados não encontrado no caminho especificado: {file_path}")
        return

    logger.info(f"Iniciando leitura e streaming do arquivo: {file_path}")
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for count, row in enumerate(reader, start=1):
                payload = parse_and_format_row(row)
                
                # Transforma o dicionário/modelo em JSON
                event_data_batch = client.create_batch()
                json_string = json.dumps(payload)
                event_data_batch.add(EventData(json_string))
                
                # Envia o evento de forma assíncrona/real-time
                client.send_batch(event_data_batch)
                
                # Log estruturado a cada 100 eventos para não saturar o console, mantendo rastreabilidade
                if count % 100 == 0 or count == 1:
                    logger.info(f"Status do Pipeline: {count} eventos enviados com sucesso até o momento.")
                    
    except EventHubError as eh_err:
        logger.error(f"Erro crítico na comunicação com o Azure Event Hubs: {eh_err}")

    except Exception as err:
        logger.error(f"Erro inesperado durante o processamento do stream: {err}")


def main():
    logger.info("Inicializando o cliente produtor do Azure Event Hubs...")
    if not EVENT_HUB_CONNECTION_STR:
        logger.critical("AZURE_EVENTHUB_CONNECTION_STRING não configurada.")
        return
    
    if not CSV_FILE_PATH:
        logger.critical("CSV_FILE_PATH não configurada.")
        return
    try:
        producer_client = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR,
            eventhub_name=EVENT_HUB_NAME
        )
        
        with producer_client:
            stream_transactions_from_csv(CSV_FILE_PATH, producer_client)
        
        logger.info("Processamento de carga concluído com sucesso.")
        
    except Exception as err:
        logger.critical(f"Falha ao inicializar o microsserviço: {err}")
        sys.exit(1)

if __name__ == "__main__":
    main()