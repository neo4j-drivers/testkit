from tests.neo4j.shared import get_server_info
from tests.shared import (
    get_driver_features,
    new_backend,
)


def main():
    backend = new_backend()
    features = get_driver_features(backend, silence_error=False)

    info = get_server_info()
    common_versions = info.common_protocol_versions(features)
    if common_versions:
        print("Common protocol versions:")
        print(f"  {' '.join(map(str, common_versions))}")
        exit(0)
    else:
        print("No common protocol versions found.")
        exit(99)


if __name__ == "__main__":
    main()
