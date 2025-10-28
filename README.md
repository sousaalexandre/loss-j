# LOSS-J RAG System

## Prerequisites

- Python >= 3.8
- OpenAI API key

## Setup
1. Clone this repository

2. Create a virtual environment:
    ```bash
    python -m venv venv
    ```

3. Activate virtual environment:
    ```bash
    source venv/bin/activate
    ```

4. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

5. Configure environment variables:
    ```bash
    cp .env.example .env
    ```
Edit [.env](.env) with you API keys and credentials.



## Running

### Visual Interface
- With hot reload (recommended for dev):
    ```bash
    streamlit run main.py --server.runOnSave true
    ```
- Simple run:
    ```bash
    streamlit run main.py
    ```
Access the app at [http://localhost:8501](http://localhost:8501).


### Comparison of Results

1. Edit [query.json](query.json) with your test queries and expected responses.

2. Run the evaluation script:
    ```bash
    run test-eval.py
    ```

3. View results in the web interface under the **Results Comparison** page.


---
[https://doi.org/10.54499/2024.07619.IACDC](https://doi.org/10.54499/2024.07619.IACDC)