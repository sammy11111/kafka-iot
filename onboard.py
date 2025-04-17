import os
import subprocess
from shutil import copyfile


ENV_TEMPLATE = ".env.template"
ENV_FILE = ".env"


def copy_env_file():
    if not os.path.exists(ENV_FILE):
        print(f"Creating {ENV_FILE} from template...")
        copyfile(ENV_TEMPLATE, ENV_FILE)
    else:
        print(f"{ENV_FILE} already exists. Skipping copy.")


def run_make_dev_up():
    print("Starting development environment with Makefile...")
    subprocess.run(["make", "dev-up"])


def main():
    print("\n========== Kafka IoT Dev Environment Setup ==========")
    copy_env_file()
    run_make_dev_up()
    print("\n✅ Setup complete. Modify .env if needed before continuing development.")


if __name__ == "__main__":
    main()