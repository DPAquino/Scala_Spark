#!/bin/bash --login

# -------------------------------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------------------------------

function get_quantity_instances_docker(){
   #PARM1 - Container Image Name
	 docker ps -a | grep ${container_image} | awk '{print $1}' | wc -l
}

function remove_unnecessary_docker_instances(){
	 #PARM1 - Container Image
   if [ $quantity -ge 1 ]; then {
	 echo "Docker Instances found that are no longer needed and will be removed:"
   docker ps -a | grep ${container_image} | awk '{print $1}' | xargs docker rm -f
   }
fi
}

function run_docker_elixir_locust(){
	 #PARM1 - Endpoint
   #PARM2 - Number of workers to spawn. This is one by default. Be careful with large values as it might
   #PARM3 – Number of requests to perform by each worker. Default is 10.
   docker run ${USE_TTY} -i -t=false ${container_image} bash -c "git clone https://github.com/katafrakt/locust.git; \
		                                                             cd locust ;                                    \
                                                                     echo Y |                                       \
                                                                     mix deps.get ;                                 \
                                                                     echo Y | mix escript.build ;                   \
                                                                     ./locust ${1} -c ${2} -n ${3} | \
                                                                     tee result-performance-tests.txt"
}

# -------------------------------------------------------------------------------------------------
# START
# -------------------------------------------------------------------------------------------------

endpoint=${1}
concurrency=${2}
number=${3}

container_image="elixir"
docker pull ${container_image}

set +e

run_docker_elixir_locust ${endpoint} ${concurrency} ${number}

quantity=`get_quantity_instances_docker ${container_image}`

remove_unnecessary_docker_instances ${container_image}

set -e

# -------------------------------------------------------------------------------------------------
# END
# -------------------------------------------------------------------------------------------------

exit 0
