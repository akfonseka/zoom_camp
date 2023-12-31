from prefect.infrastructure.docker import DockerContainer

#alternative to creating a DockerContainer block in the Prefect UI
docker_block = DockerContainer(
    image='akfonseka/prefect:zoom'
    ,image_pull_policy='ALWAYS'
    ,auto_remove=True
)

docker_block.save('zoom', overwrite=True)