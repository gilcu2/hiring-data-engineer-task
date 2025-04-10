import docker

try:
    # Create a Docker client instance.
    client = docker.from_env()

    image_name = "gilcu2/pipeline_kpi_etc:latest"  # Replace with the name of the image you want to pull

    try:
        print(f"Attempting to pull image: {image_name}...")
        # The pull() method downloads the image from Docker Hub or a configured registry.
        image = client.images.pull(image_name)
        print(f"Successfully pulled image: {image.tags[0] if image.tags else image.id[:12]}")

    except docker.errors.ImageNotFound:
        print(f"Error: Image '{image_name}' not found on Docker Hub or configured registries.")
    except docker.errors.APIError as e:
        print(f"Error interacting with the Docker daemon or registry: {e}")

except docker.errors.DockerException as e:
    print(f"Error connecting to the Docker daemon: {e}")