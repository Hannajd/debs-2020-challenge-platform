# DEBS Grand Challenge Frontend

The front-end part of the DEBS Grand Challenge is comprised of the scheduler, which periodically checks if an team images have been updated on DockerHub and the controller component, which handles result storage and display, provides a UI for the admins, displays the scoreboard and acts as a mediator between the scheduler and the manager components.


## Configuration

Ensure that all variables are set as expected in [.env file](container-config.env)!

`Important!` Don't forget to change the SECRET_KEY there for production.

After that, start the containers with `docker-compose up --build`. 

To add new teams you need to run the script `create_access.py` from the `frontend` container. To do this, run
```bash
docker exec -it frontend /bin/bash
```
And then run the script with `python3 create_access.py` and follow the instructions.

The `dbserver` container is running the MySQL DB and you can enter it as above in case you need to edit the database directly.
