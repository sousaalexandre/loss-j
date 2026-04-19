import re
import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.title("Comparação de Resultados")
st.markdown("Selecione um arquivo de resultados de teste para ver a tabela de comparação.")

st.info("💡 Para gerar novos resultados: Edite **query.json** e execute **test-eval.py** com as suas questões e respostas esperadas.")

TABLE_CSS = """
<style>
    table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
    }
    th, td {
        padding: 8px 12px;
        width: 25%;
        word-wrap: break-word;
        vertical-align: top;
    }
    th {
        font-weight: bold;
        text-align: right;
    }
    td {
        text-align: left;
    }
    td:first-child {
        font-weight: bold;
    }
    td:last-child {
        text-align: right;
    }
</style>
"""

def color_acc(val):
    """Format accuracy value with color-coded emoji indicator.
    
    Assigns emoji indicators based on accuracy threshold: 
    🟢 for ≥80%
    🟡 for ≥60%
    🟠 for ≥40%
    🔴 for <40%
    """
    try:
        val = float(val)
        if val >= 80:
            emoji = '🟢'
        elif val >= 60:
            emoji = '🟡'
        elif val >= 40:
            emoji = '🟠'
        else:
            emoji = '🔴'
        return f'{val} {emoji}'
    except ValueError:
        return val

def get_metric_emoji(val):
    """Return emoji indicator based on numeric accuracy value.
    
    Assigns emoji: 🟢 for ≥80%, 🟡 for ≥60%, 🟠 for ≥40%, 🔴 for <40%.
    """
    if val >= 80:
        return '🟢'
    elif val >= 60:
        return '🟡'
    elif val >= 40:
        return '🟠'
    else:
        return '🔴'

def detect_query_id_column(df: pd.DataFrame) -> str | None:
    """Detect the query identifier column name in a results dataframe."""
    for candidate in ['Query ID', 'id', 'ID', 'query_id', 'QueryId']:
        if candidate in df.columns:
            return candidate
    return None

outputs_dir = 'outputs/results/'
if os.path.exists(outputs_dir):
    csv_files = [f for f in os.listdir(outputs_dir) if f.endswith('.csv')]
    csv_files.sort(key=lambda f: int(re.search(r'v(\d+)', f).group(1)) if re.search(r'v(\d+)', f) else 0, reverse=True)
    if csv_files:
        selected_file = st.selectbox("Escolha um arquivo de resultados", csv_files)
        file_path = os.path.join(outputs_dir, selected_file)
        
        df = pd.read_csv(file_path)
        
        if all(col in df.columns for col in ['Query', 'Expected Response', 'Received Response', 'Meaning Acc (%)']):
            query_id_col = detect_query_id_column(df)
            if query_id_col:
                df[query_id_col] = pd.to_numeric(df[query_id_col], errors='coerce')
                df = df.sort_values(by=query_id_col, ascending=True, na_position='last')
                if query_id_col != 'Query ID':
                    df = df.rename(columns={query_id_col: 'Query ID'})
                    query_id_col = 'Query ID'

            # metrics
            acc_values = pd.to_numeric(df['Meaning Acc (%)'], errors='coerce').dropna()
            
            if len(acc_values) > 0:
                mean_acc = acc_values.mean()
                median_acc = acc_values.median()
                min_acc = acc_values.min()
                max_acc = acc_values.max()
                std_acc = acc_values.std()
                
                excellent = (acc_values >= 80).sum()
                good = ((acc_values >= 60) & (acc_values < 80)).sum()
                fair = ((acc_values >= 40) & (acc_values < 60)).sum()
                poor = (acc_values < 40).sum()
                pass_rate = ((acc_values >= 60).sum() / len(acc_values) * 100)
                excellence_rate = ((acc_values >= 80).sum() / len(acc_values) * 100)
                                  
                with st.expander("📊 Ver Métricas de Desempenho", expanded=True):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        st.metric(f"Média {get_metric_emoji(mean_acc)}", f"{mean_acc:.1f}%")
                    
                    with col2:
                        st.metric(f"Mediana {get_metric_emoji(median_acc)}", f"{median_acc:.1f}%")
                    
                    with col3:
                        st.metric("Mínimo", f"{min_acc:.1f}%")
                    
                    with col4:
                        st.metric("Máximo", f"{max_acc:.1f}%")
                    
                    with col5:
                        st.metric("Desvio Padrão", f"{std_acc:.2f}")
                    
                    st.markdown("---")
                    
                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                    
                    with col1:
                        st.metric("Total", len(acc_values))
                    
                    with col2:
                        st.metric("🟢 ≥80%", excellent)
                    
                    with col3:
                        st.metric("🟡 60-79%", good)
                    
                    with col4:
                        st.metric("🟠 40-59%", fair)
                    
                    with col5:
                        st.metric("🔴 <40%", poor)
                    
                    with col6:
                        st.metric("Taxa ≥60%", f"{pass_rate:.1f}%")
            
            st.markdown("---")
            st.subheader(f"Resultados de {selected_file}")
            display_columns = ['Query', 'Expected Response', 'Received Response', 'Meaning Acc (%)']
            if 'Query ID' in df.columns:
                display_columns = ['Query ID'] + display_columns

            display_df = df[display_columns]
            display_df = display_df.replace('\n', '<br>', regex=True)
            display_df['Meaning Acc (%)'] = display_df['Meaning Acc (%)'].apply(color_acc)
            table_html = display_df.to_html(index=False, escape=False)
        
            st.markdown(TABLE_CSS, unsafe_allow_html=True)
            st.markdown(table_html, unsafe_allow_html=True)
        else:
            st.error("The selected file does not have the required columns.")
    else:
        st.info("No results files found in outputs/. Run the test script first.")
else:
    st.error("Outputs directory not found.")