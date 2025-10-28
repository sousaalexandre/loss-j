import streamlit as st
import pandas as pd
import os

st.title("Comparação de Resultados (Beta)")
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

outputs_dir = 'outputs/results/'
if os.path.exists(outputs_dir):
    csv_files = sorted([f for f in os.listdir(outputs_dir) if f.endswith('.csv')], key=lambda f: f.split('_')[2] + f.split('_')[3].split('.')[0], reverse=True)
    if csv_files:
        selected_file = st.selectbox("Escolha um arquivo de resultados", csv_files)
        file_path = os.path.join(outputs_dir, selected_file)
        
        df = pd.read_csv(file_path)
        st.subheader(f"Resultados de {selected_file}")
        
        if all(col in df.columns for col in ['Query', 'Received Response', 'Expected Response', 'Meaning Acc (%)']):
            display_df = df[['Query', 'Received Response', 'Expected Response', 'Meaning Acc (%)']]

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