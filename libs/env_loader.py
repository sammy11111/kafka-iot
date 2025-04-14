import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_environment():
    """
    Set up the application environment by loading configuration and setting up paths.

    Returns:
        dict: Environment configuration details
    """
    # -----------------------------------
    # Step 1: Determine project root path
    # -----------------------------------
    project_root = os.getenv("PROJECT_ROOT", "/app")

    # Fallback for local development with multiple strategies
    if not os.path.exists(project_root):
        # Try relative path from current file
        candidate_path = Path(__file__).resolve().parent.parent.parent
        if candidate_path.exists():
            project_root = str(candidate_path)
        else:
            # Try current working directory as fallback
            cwd = Path.cwd()
            if (cwd / "libs").exists():
                project_root = str(cwd)
            else:
                logger.warning(f"Could not determine project root. Using current directory: {cwd}")
                project_root = str(cwd)

    logger.info(f"Using project root: {project_root}")

    # -----------------------------------
    # Step 2: Load environment variables
    # -----------------------------------
    # First try standard .env
    env_files_loaded = 0
    dotenv_path = Path(project_root) / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        env_files_loaded += 1
        logger.info(f"Loaded environment from: {dotenv_path}")

    # Then try environment-specific overrides
    env_name = os.getenv("ENV", "development")
    env_specific_path = Path(project_root) / f".env.{env_name}"
    if env_specific_path.exists():
        load_dotenv(env_specific_path, override=True)
        env_files_loaded += 1
        logger.info(f"Loaded environment-specific overrides from: {env_specific_path}")

    # Finally try local overrides
    local_dotenv_path = Path(project_root) / ".env.local"
    if local_dotenv_path.exists():
        load_dotenv(local_dotenv_path, override=True)
        env_files_loaded += 1
        logger.info(f"Loaded local overrides from: {local_dotenv_path}")

    if env_files_loaded == 0:
        logger.warning(f"No .env files found in {project_root}")

    # -----------------------------------
    # Step 3: Add shared libraries to sys.path
    # -----------------------------------
    libs_path = Path(project_root) / "libs"
    if str(libs_path) not in sys.path and libs_path.exists():
        sys.path.insert(0, str(libs_path))
        logger.info(f"Added libs path to sys.path: {libs_path}")
    elif not libs_path.exists():
        logger.warning(f"Libs directory not found: {libs_path}")

    # -----------------------------------
    # Step 4: Validate critical environment variables
    # -----------------------------------
    critical_vars = ["MONGO_URI", "KAFKA_BOOTSTRAP_SERVER"]
    missing_vars = [var for var in critical_vars if not os.getenv(var)]

    if missing_vars:
        message = f"Missing critical environment variables: {', '.join(missing_vars)}"
        logger.error(message)

        # Option: fail fast for missing critical variables
        # Uncomment the line below to enforce strict configuration
        # raise EnvironmentError(message)

    return {
        "project_root": project_root,
        "env_files_loaded": env_files_loaded,
        "environment": env_name,
        "libs_path_added": str(libs_path) in sys.path,
        "missing_critical_vars": missing_vars
    }


# Run setup when this module is imported
env_config = setup_environment()

PROJECT_ROOT = env_config["project_root"]