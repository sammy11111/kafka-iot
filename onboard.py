import os
import shutil
import subprocess


def copy_env():
    if not os.path.exists(".env") and os.path.exists(".env.template"):
        shutil.copy(".env.template", ".env")
        print("✅ .env file created from template.")
    else:
        print("ℹ️ .env already exists or no template found.")


def run_make_up():
    print("🚀 Starting services with `make up`...")
    subprocess.run(["make", "up"])


if __name__ == "__main__":
    copy_env()
    run_make_up()
