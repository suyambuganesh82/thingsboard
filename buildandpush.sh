#
# Copyright © 2016-2024 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#mvn clean license:format install -DskipTests -Ddockerfile.skip=false -T 8
mvn clean license:format install -DskipTests -Ddockerfile.skip=false -U

services=("tb-http-transport" "tb-mqtt-transport" "tb-node" "tb-web-ui" "tb-js-executor" "tb-vc-executor")

VERSION=3.7.0

for service in "${services[@]}"
do
    echo $service
    docker tag thingsboard/$service:latest europe-docker.pkg.dev/sangam-iot/docker/$service:$VERSION
    docker push europe-docker.pkg.dev/sangam-iot/docker/$service:$VERSION
done
