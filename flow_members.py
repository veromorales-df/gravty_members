import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression

class GenderIdentification(beam.DoFn):
    def __init__(self):
        super().__init__()
        self.model = None
        self.vectorizer = None

    def setup(self):
        self.model = LogisticRegression()
        self.vectorizer = CountVectorizer(analyzer='char')

    def process(self, element):
        name = element['first_name']
        gender = self.model.predict(self.vectorizer.transform([name]))
        element['gender_by_name'] = 'F' if gender == 'female' else 'M'
        yield element

    def teardown(self):
        # Aquí puedes realizar cualquier limpieza necesaria después de procesar los elementos
        pass

def run_pipeline():
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Paso 1: Leer datos desde BigQuery
    source_query = """
        SELECT first_name, gender
        FROM `daf-datalake-dev-raw.coa_gravty.tbl_members_member_test`
    """
    data = p | 'ReadData' >> ReadFromBigQuery(query=source_query)

    # Paso 2: Obtener los datos de entrenamiento
    training_data = (
        data
        | 'ExtractTrainingData' >> beam.Map(lambda x: x['first_name'])
        | 'CollectTrainingData' >> beam.combiners.ToList()
    )

    target = (
        data
        | 'ExtractTarget' >> beam.Map(lambda x: x['gender'])
        | 'CollectTarget' >> beam.combiners.ToList()
    )

    # Paso 3: Aplicar la transformación y agregar la nueva columna
    transformed_data = (
        data
        | 'TransformData' >> beam.ParDo(GenderIdentification())
    )

    # Paso 4: Escribir los datos en una nueva tabla de BigQuery
    destination_table = "daf-datalake-dev-raw.coa_gravty.tbl_members_member_training"
    transformed_data | 'WriteToBigQuery' >> WriteToBigQuery(table=destination_table)

    # Ejecutar el pipeline en Dataflow
    p.run().wait_until_finish()

if __name__ == '__main__':
    run_pipeline()
"""
A continuación se los comandos que se usan para crear el template
y para ejecutar el job desde el template.
​
---------------------------------------------------------
Primero para crear un template que jale toda una tabla origen:
​
python -m create_table \
    --runner DataflowRunner \
    --project daf-datalake-dev-raw \
    --staging_location gs://coa_gravty.tbl_members_member_test\
    --temp_location gs://coa_gravty.tbl_members_member_test/temp \
    --template_location gs://coa_gravty.tbl_members_member_test /templates/create_table \
    --region us-east1 
    --schema:

Para ejecutar el job con el template anterior:
​
gcloud dataflow jobs run job-create_table \
--project daf-datalake-dev-raw \
--gcs-location  gs://coa_gravty.tbl_members_member_training \
--region us-east1 \
--parameters \
^~^query="SELECT first_name FROM `{project}.{dataset}.{input_table}" 
~table_dst=daf-datalake-dev-trusted.coa_gravty.tbl_members_member_training 
​
​
​
"""