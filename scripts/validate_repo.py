from pathlib import Path

import nbformat
import yaml

ROOT = Path(__file__).resolve().parents[1]


def validate_notebooks() -> None:
    notebooks_dir = ROOT / "notebooks"
    notebooks = sorted(notebooks_dir.glob("*.ipynb"))

    if not notebooks:
        raise FileNotFoundError(f"Aucun notebook trouvé dans {notebooks_dir}")

    for notebook_path in notebooks:
        with notebook_path.open("r", encoding="utf-8") as fh:
            nb = nbformat.read(fh, as_version=4)

        if not isinstance(nb.cells, list):
            raise ValueError(f"Notebook invalide : {notebook_path}")

        print(f"OK notebook: {notebook_path.relative_to(ROOT)}")


def validate_compose() -> None:
    compose_path = ROOT / "docker" / "docker-compose.yml"

    if not compose_path.exists():
        raise FileNotFoundError(f"Compose introuvable : {compose_path}")

    with compose_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not isinstance(data, dict) or "services" not in data:
        raise ValueError(f"docker-compose.yml invalide : {compose_path}")

    print(f"OK docker compose: {compose_path.relative_to(ROOT)}")


def main() -> None:
    validate_notebooks()
    validate_compose()
    print("Validation CI OK")


if __name__ == "__main__":
    main()
