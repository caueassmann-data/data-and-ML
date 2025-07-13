from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import os
import logging
from calendar import monthrange

# Configurações padrão do DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Criação do DAG
dag = DAG(
    'pipeline_vendas_complexo',
    default_args=default_args,
    description='Pipeline com múltiplos CSVs: diário, mensal e histórico',
    schedule_interval='0 2 * * *',  # Todo dia às 2h
    catchup=False,
    max_active_runs=1,
)

# Configurações
CRM_DB_PATH = '/opt/dados/crm_vendas.db'
CSV_BASE_PATH = '/opt/shared/tableau'
BACKUP_PATH = '/opt/backup/vendas'

def extrair_dados_vendas(**context):
    """
    Extrai dados de vendas com diferentes períodos
    """
    try:
        data_execucao = context['execution_date']
        
        # Definir períodos
        ontem = (data_execucao - timedelta(days=1)).strftime('%Y-%m-%d')
        inicio_mes = data_execucao.replace(day=1).strftime('%Y-%m-%d')
        inicio_mes_anterior = (data_execucao.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
        
        logging.info(f"Extraindo dados - Ontem: {ontem}, Início mês: {inicio_mes}")
        
        conn = sqlite3.connect(CRM_DB_PATH)
        
        # Query base com todos os dados necessários
        query_base = """
        SELECT 
            v.id_venda,
            v.data_venda,
            v.valor_total,
            v.quantidade,
            v.status_venda,
            p.nome_produto,
            p.categoria,
            p.preco_unitario,
            c.nome_cliente,
            c.regiao,
            c.segmento,
            ve.nome_vendedor,
            ve.equipe,
            strftime('%Y', v.data_venda) as ano,
            strftime('%m', v.data_venda) as mes,
            strftime('%Y-%m', v.data_venda) as ano_mes
        FROM vendas v
        JOIN produtos p ON v.id_produto = p.id_produto
        JOIN clientes c ON v.id_cliente = c.id_cliente
        JOIN vendedores ve ON v.id_vendedor = ve.id_vendedor
        WHERE v.status_venda = 'CONCLUIDA'
        AND v.data_venda >= '{inicio_mes_anterior}'
        ORDER BY v.data_venda;
        """
        
        df_completo = pd.read_sql_query(query_base.format(inicio_mes_anterior=inicio_mes_anterior), conn)
        conn.close()
        
        logging.info(f"Extraídos {len(df_completo)} registros totais")
        
        # Separar dados por período
        dados_periodos = {
            'ontem': ontem,
            'inicio_mes': inicio_mes,
            'dados_completos': df_completo.to_json(orient='records'),
            'data_execucao': data_execucao.strftime('%Y-%m-%d')
        }
        
        return dados_periodos
        
    except Exception as e:
        logging.error(f"Erro na extração: {str(e)}")
        raise

def processar_csv_diario(**context):
    """
    Processa CSV com dados do dia anterior
    """
    try:
        dados_periodos = context['task_instance'].xcom_pull(task_ids='extrair_dados')
        df_completo = pd.read_json(dados_periodos['dados_completos'], orient='records')
        ontem = dados_periodos['ontem']
        
        # Filtrar apenas ontem
        df_ontem = df_completo[df_completo['data_venda'] == ontem].copy()
        
        logging.info(f"Processando {len(df_ontem)} registros do dia {ontem}")
        
        if len(df_ontem) == 0:
            logging.warning(f"Nenhuma venda encontrada para {ontem}")
            # Criar DataFrame vazio com estrutura
            df_ontem = pd.DataFrame(columns=[
                'id_venda', 'data_venda', 'valor_total', 'quantidade', 
                'nome_produto', 'categoria', 'nome_cliente', 'regiao',
                'nome_vendedor', 'equipe', 'ticket_medio', 'categoria_venda'
            ])
        else:
            # Transformações específicas para dados diários
            df_ontem['data_venda'] = pd.to_datetime(df_ontem['data_venda'])
            df_ontem['valor_total'] = df_ontem['valor_total'].astype(float).round(2)
            df_ontem['ticket_medio'] = df_ontem['valor_total'] / df_ontem['quantidade']
            df_ontem['categoria_venda'] = df_ontem['valor_total'].apply(
                lambda x: 'Alto' if x > 1000 else 'Médio' if x > 500 else 'Baixo'
            )
            df_ontem['dia_semana'] = df_ontem['data_venda'].dt.day_name()
        
        # Salvar CSV diário
        csv_path = f"{CSV_BASE_PATH}/vendas_diarias.csv"
        df_ontem.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        logging.info(f"CSV diário salvo: {csv_path}")
        
        return {
            'arquivo': csv_path,
            'registros': len(df_ontem),
            'valor_total': float(df_ontem['valor_total'].sum()) if len(df_ontem) > 0 else 0
        }
        
    except Exception as e:
        logging.error(f"Erro no processamento diário: {str(e)}")
        raise

def processar_csv_mensal_acumulado(**context):
    """
    Processa CSV com dados acumulados do mês atual
    """
    try:
        dados_periodos = context['task_instance'].xcom_pull(task_ids='extrair_dados')
        df_completo = pd.read_json(dados_periodos['dados_completos'], orient='records')
        inicio_mes = dados_periodos['inicio_mes']
        
        # Filtrar do início do mês até ontem
        df_mes = df_completo[df_completo['data_venda'] >= inicio_mes].copy()
        
        logging.info(f"Processando {len(df_mes)} registros do mês atual")
        
        if len(df_mes) == 0:
            logging.warning("Nenhuma venda encontrada no mês atual")
            df_agregado = pd.DataFrame()
        else:
            # Agregações por diferentes dimensões
            df_mes['data_venda'] = pd.to_datetime(df_mes['data_venda'])
            df_mes['valor_total'] = df_mes['valor_total'].astype(float)
            df_mes['quantidade'] = df_mes['quantidade'].astype(int)
            
            # Agregação por vendedor
            vendedor_mes = df_mes.groupby('nome_vendedor').agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            vendedor_mes.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            vendedor_mes['tipo_agregacao'] = 'vendedor'
            vendedor_mes['nome_dimensao'] = vendedor_mes.index
            vendedor_mes = vendedor_mes.reset_index(drop=True)
            
            # Agregação por produto
            produto_mes = df_mes.groupby('nome_produto').agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            produto_mes.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            produto_mes['tipo_agregacao'] = 'produto'
            produto_mes['nome_dimensao'] = produto_mes.index
            produto_mes = produto_mes.reset_index(drop=True)
            
            # Agregação por região
            regiao_mes = df_mes.groupby('regiao').agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            regiao_mes.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            regiao_mes['tipo_agregacao'] = 'regiao'
            regiao_mes['nome_dimensao'] = regiao_mes.index
            regiao_mes = regiao_mes.reset_index(drop=True)
            
            # Agregação por dia
            dia_mes = df_mes.groupby(df_mes['data_venda'].dt.date).agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            dia_mes.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            dia_mes['tipo_agregacao'] = 'dia'
            dia_mes['nome_dimensao'] = dia_mes.index.astype(str)
            dia_mes = dia_mes.reset_index(drop=True)
            
            # Combinar todas as agregações
            df_agregado = pd.concat([vendedor_mes, produto_mes, regiao_mes, dia_mes], ignore_index=True)
            
            # Adicionar metadados
            df_agregado['mes_referencia'] = inicio_mes
            df_agregado['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Reordenar colunas
            df_agregado = df_agregado[[
                'tipo_agregacao', 'nome_dimensao', 'total_vendas', 'ticket_medio',
                'num_vendas', 'total_itens', 'mes_referencia', 'data_processamento'
            ]]
        
        # Salvar CSV mensal
        csv_path = f"{CSV_BASE_PATH}/vendas_mensal_acumulado.csv"
        df_agregado.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        logging.info(f"CSV mensal salvo: {csv_path}")
        
        return {
            'arquivo': csv_path,
            'registros': len(df_agregado),
            'valor_total_mes': float(df_mes['valor_total'].sum()) if len(df_mes) > 0 else 0
        }
        
    except Exception as e:
        logging.error(f"Erro no processamento mensal: {str(e)}")
        raise

def processar_csv_historico_meses(**context):
    """
    Processa CSV com dados históricos de meses anteriores
    """
    try:
        dados_periodos = context['task_instance'].xcom_pull(task_ids='extrair_dados')
        df_completo = pd.read_json(dados_periodos['dados_completos'], orient='records')
        data_execucao = datetime.strptime(dados_periodos['data_execucao'], '%Y-%m-%d')
        
        # Filtrar apenas meses completos anteriores (não o atual)
        mes_atual = data_execucao.strftime('%Y-%m')
        df_historico = df_completo[df_completo['ano_mes'] < mes_atual].copy()
        
        logging.info(f"Processando {len(df_historico)} registros históricos")
        
        if len(df_historico) == 0:
            logging.warning("Nenhum dado histórico encontrado")
            df_historico_agregado = pd.DataFrame()
        else:
            df_historico['valor_total'] = df_historico['valor_total'].astype(float)
            df_historico['quantidade'] = df_historico['quantidade'].astype(int)
            
            # Agregação por mês e vendedor
            historico_vendedor = df_historico.groupby(['ano_mes', 'nome_vendedor']).agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            historico_vendedor.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            historico_vendedor['tipo_agregacao'] = 'vendedor_mes'
            historico_vendedor = historico_vendedor.reset_index()
            historico_vendedor['dimensao'] = historico_vendedor['nome_vendedor']
            
            # Agregação por mês e produto
            historico_produto = df_historico.groupby(['ano_mes', 'nome_produto']).agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            historico_produto.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            historico_produto['tipo_agregacao'] = 'produto_mes'
            historico_produto = historico_produto.reset_index()
            historico_produto['dimensao'] = historico_produto['nome_produto']
            
            # Agregação por mês total
            historico_total = df_historico.groupby('ano_mes').agg({
                'valor_total': ['sum', 'mean', 'count'],
                'quantidade': 'sum'
            }).round(2)
            historico_total.columns = ['total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']
            historico_total['tipo_agregacao'] = 'total_mes'
            historico_total = historico_total.reset_index()
            historico_total['dimensao'] = 'TOTAL'
            
            # Combinar agregações
            df_historico_agregado = pd.concat([
                historico_vendedor[['ano_mes', 'dimensao', 'tipo_agregacao', 'total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']],
                historico_produto[['ano_mes', 'dimensao', 'tipo_agregacao', 'total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']],
                historico_total[['ano_mes', 'dimensao', 'tipo_agregacao', 'total_vendas', 'ticket_medio', 'num_vendas', 'total_itens']]
            ], ignore_index=True)
            
            # Adicionar metadados
            df_historico_agregado['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Ordenar por mês
            df_historico_agregado = df_historico_agregado.sort_values(['ano_mes', 'tipo_agregacao', 'dimensao'])
        
        # Salvar CSV histórico
        csv_path = f"{CSV_BASE_PATH}/vendas_historico_meses.csv"
        df_historico_agregado.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        logging.info(f"CSV histórico salvo: {csv_path}")
        
        return {
            'arquivo': csv_path,
            'registros': len(df_historico_agregado),
            'meses_processados': len(df_historico_agregado['ano_mes'].unique()) if len(df_historico_agregado) > 0 else 0
        }
        
    except Exception as e:
        logging.error(f"Erro no processamento histórico: {str(e)}")
        raise

def gerar_relatorio_resumo(**context):
    """
    Gera relatório resumo com estatísticas dos 3 CSVs
    """
    try:
        # Coleta resultados das tasks anteriores
        resultado_diario = context['task_instance'].xcom_pull(task_ids='processar_csv_diario')
        resultado_mensal = context['task_instance'].xcom_pull(task_ids='processar_csv_mensal')
        resultado_historico = context['task_instance'].xcom_pull(task_ids='processar_csv_historico')
        
        # Cria relatório resumo
        relatorio = {
            'data_processamento': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'csv_diario': {
                'arquivo': resultado_diario['arquivo'],
                'registros': resultado_diario['registros'],
                'valor_total': resultado_diario['valor_total']
            },
            'csv_mensal': {
                'arquivo': resultado_mensal['arquivo'],
                'registros': resultado_mensal['registros'],
                'valor_total_mes': resultado_mensal['valor_total_mes']
            },
            'csv_historico': {
                'arquivo': resultado_historico['arquivo'],
                'registros': resultado_historico['registros'],
                'meses_processados': resultado_historico['meses_processados']
            }
        }
        
        # Salva relatório
        import json
        relatorio_path = f"{CSV_BASE_PATH}/relatorio_processamento.json"
        with open(relatorio_path, 'w', encoding='utf-8') as f:
            json.dump(relatorio, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Relatório salvo: {relatorio_path}")
        
        return relatorio
        
    except Exception as e:
        logging.error(f"Erro na geração do relatório: {str(e)}")
        raise

# Definição das tasks
task_extrair = PythonOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados_vendas,
    dag=dag,
)

task_csv_diario = PythonOperator(
    task_id='processar_csv_diario',
    python_callable=processar_csv_diario,
    dag=dag,
)

task_csv_mensal = PythonOperator(
    task_id='processar_csv_mensal',
    python_callable=processar_csv_mensal_acumulado,
    dag=dag,
)

task_csv_historico = PythonOperator(
    task_id='processar_csv_historico',
    python_callable=processar_csv_historico_meses,
    dag=dag,
)

task_relatorio = PythonOperator(
    task_id='gerar_relatorio',
    python_callable=gerar_relatorio_resumo,
    dag=dag,
)

# Notificação final
task_notificar = EmailOperator(
    task_id='notificar_conclusao',
    to=['equipe-bi@empresa.com'],
    subject='Pipeline Vendas Complexo - Concluído',
    html_content="""
    <h3>Pipeline de Vendas Complexo Executado</h3>
    <p>Data: {{ ds }}</p>
    <p>Arquivos gerados:</p>
    <ul>
        <li>vendas_diarias.csv - Dados do dia anterior</li>
        <li>vendas_mensal_acumulado.csv - Dados acumulados do mês</li>
        <li>vendas_historico_meses.csv - Histórico de meses anteriores</li>
    </ul>
    <p>Dashboards Tableau podem ser atualizados.</p>
    """,
    dag=dag,
)

# Definição das dependências
task_extrair >> [task_csv_diario, task_csv_mensal, task_csv_historico] >> task_relatorio >> task_notificar
