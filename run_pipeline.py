import argparse, subprocess, sys
def run(module,*args):
    cmd=[sys.executable,"-m",module,*args]
    print("\n>"," ".join(cmd)); subprocess.run(cmd,check=True)
if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--events",type=int,default=1000000); a=p.parse_args()
    run("src.generators.generate_reference_data")
    run("src.generators.generate_telemetry","--events",str(a.events))
    run("src.ingestion.raw_to_bronze")
    run("src.transformations.bronze_to_silver")
    run("src.transformations.build_gold_metrics")
    run("src.quality.validate_pipeline")
    print("\nPipeline completed successfully.")
