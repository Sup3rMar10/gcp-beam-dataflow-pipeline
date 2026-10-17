"""Entry point the Dataflow Flex Template launcher runs. Reference only: never launched on Dataflow."""

from payments_beam.pipeline import run_stream

if __name__ == "__main__":
    run_stream()
