import argparse

def greet(name: str = "Filesage") -> str:
    """Return a greeting string."""
    return f"Hello {name}"

def main() -> None:
    parser = argparse.ArgumentParser(prog="filesage")
    parser.add_argument("-n", "--name", default="Filesage", help="Name to greet")
    args = parser.parse_args()
    print(greet(args.name))

if __name__ == "__main__":
    main()
