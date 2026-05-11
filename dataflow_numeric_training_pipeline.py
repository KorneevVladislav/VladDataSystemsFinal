import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "root-patrol-495501-r3"
DATASET = "trendfusion"

INPUT_TABLE = f"{PROJECT_ID}.{DATASET}.next_day_views_training_data"
OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET}.next_day_views_numeric_training_data"


NUMERIC_COLUMNS = [
    "trend_avg_30d",
    "youtube_avg_30d",
    "trend_avg_14d",
    "youtube_avg_14d",
    "trend_avg_7d",
    "youtube_avg_7d",
    "trend_avg_1d",
    "youtube_avg_1d",
    "avg_likes_30d",
    "avg_dislikes_30d",
    "avg_comments_30d",
    "youtube_video_count",
    "next_day_youtube_avg_views",
]


class KeepOnlyNumericFields(beam.DoFn):
    def process(self, row):
        cleaned = {}

        for col in NUMERIC_COLUMNS:
            value = row.get(col)

            if value is None:
                value = 0.0

            try:
                cleaned[col] = float(value)
            except Exception:
                cleaned[col] = 0.0

        yield cleaned


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    query = f"""
    SELECT
      trend_avg_30d,
      youtube_avg_30d,
      trend_avg_14d,
      youtube_avg_14d,
      trend_avg_7d,
      youtube_avg_7d,
      trend_avg_1d,
      youtube_avg_1d,
      avg_likes_30d,
      avg_dislikes_30d,
      avg_comments_30d,
      youtube_video_count,
      next_day_youtube_avg_views
    FROM `{INPUT_TABLE}`
    WHERE next_day_youtube_avg_views IS NOT NULL
    """

    schema = {
        "fields": [
            {"name": "trend_avg_30d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "youtube_avg_30d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "trend_avg_14d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "youtube_avg_14d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "trend_avg_7d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "youtube_avg_7d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "trend_avg_1d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "youtube_avg_1d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "avg_likes_30d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "avg_dislikes_30d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "avg_comments_30d", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "youtube_video_count", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "next_day_youtube_avg_views", "type": "FLOAT", "mode": "NULLABLE"},
        ]
    }

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read training data from BigQuery" >> beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True
            )
            | "Remove tag/date and keep numeric values" >> beam.ParDo(KeepOnlyNumericFields())
            | "Write numeric training table" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    run()
