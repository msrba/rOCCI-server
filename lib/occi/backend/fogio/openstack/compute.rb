##############################################################################
#  Copyright 2011 Service Computing group, TU Dortmund
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##############################################################################

##############################################################################
# Description: Fogio Openstack Backend
# Author(s): Maik Srba
##############################################################################

require 'occi/log'
require "base64"

module OCCI
  module Backend
    class Fogio
      module Openstack
        class Compute

          attr_accessor :model

          def initialize(model)
            @model = model
          end

          def parse_backend_object(client, backend_object)
            kind = @model.get_by_id("http://schemas.ogf.org/occi/infrastructure#compute")

            flavor = client.flavors.get backend_object.attributes[:flavor]['id']

            id = backend_object.attributes[:id]

            compute = OCCI::Core::Resource.new(kind.type_identifier)
            compute.mixins << 'http://openstack.org/occi/infrastructure#compute'
            compute.id = nil

            parse_metadata(compute, backend_object.metadata)
            compute.mixins.uniq!

            if compute.id.nil? || compute.id.length <= 0
              compute.id = UUIDTools::UUID.timestamp_create.to_s
              client.set_metadata("servers", id, {'occi_attribute_occi.core.id' => compute.id.to_s})
            end

            compute.title = backend_object.attributes[:name]
            compute.attributes.occi!.compute!.cores = flavor.attributes[:vcpus]
            compute.attributes.occi!.compute!.memory = flavor.attributes[:ram]

            compute.attributes.org!.openstack!.compute!.id = id
            compute.attributes.org!.openstack!.compute!.ephemeral = flavor.attributes[:ephemeral]
            compute.attributes.org!.openstack!.compute!.flavor_id = flavor.attributes[:id]
            compute.attributes.org!.openstack!.compute!.image_id = backend_object.attributes[:image]["id"]
            compute.attributes.org!.openstack!.compute!.task_state = backend_object.attributes[:os_ext_sts_task_state] if !backend_object.attributes[:os_ext_sts_task_state].nil?
            compute.attributes.org!.openstack!.compute!.power_state = backend_object.attributes[:os_ext_sts_power_state]
            compute.attributes.org!.openstack!.compute!.created = backend_object.attributes[:created].to_s
            compute.attributes.org!.openstack!.compute!.updated = backend_object.attributes[:updated].to_s
            compute.attributes.org!.openstack!.compute!.accessIPv4 = backend_object.attributes[:accessIPv4].to_s if backend_object.attributes[:accessIPv4].to_s.length > 0
            compute.attributes.org!.openstack!.compute!.accessIPv6 = backend_object.attributes[:accessIPv6].to_s if backend_object.attributes[:accessIPv6].to_s.length > 0

            unless backend_object.attributes[:addresses]['fixed'].nil?
              compute.attributes.org!.openstack!.compute!.fixedIP = backend_object.attributes[:addresses]['fixed'][0]['addr'].to_s unless backend_object.attributes[:addresses]['fixed'].empty?
            end

            compute.check(@model)

            set_state backend_object, compute

            parse_links client, backend_object, compute

            kind.entities << compute unless kind.entities.select { |entity| entity.id == compute.id }.any?
          end

          def parse_links client, backend_object, compute
          end

          def parse_metadata(compute, metadata)
            metadata.each do |metadata|
              value = metadata.attributes[:value]
              key = metadata.attributes[:key]

              if key[0, 'occi_attribute'.length] == 'occi_attribute'
                attribute_keys = key['occi_attribute_'.length..-1].split('.')

                if attribute_keys[0] == 'cloud4e'
                  compute.mixins << 'http://cloud4e.de/occi/service#simulation'
                  compute.mixins.uniq!
                end

                attribute = compute.attributes.send "#{attribute_keys.delete_at(0)}!"
                last = attribute_keys.delete_at(-1)

                attribute_keys.each { |attribute_key| attribute = attribute.send "#{attribute_key}!" }

                attribute.send "#{last}=", value
              end
            end
          end

          def set_state(backend_object, compute)
            backend_state = backend_object.attributes[:os_ext_sts_vm_state].downcase

            OCCI::Log.debug("current VM state is: #{backend_state}")
            case backend_state
              when "active" then
                compute.attributes.occi!.compute!.state = "active"
                compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#stop http://schemas.ogf.org/occi/infrastructure/compute/action#restart http://schemas.ogf.org/occi/infrastructure/compute/action#suspend|
              when "build", "deleted", "hard_reboot", "password", "reboot", "rebuild", "rescue", "resize", "revert_resize", "shutoff", "verify_resize" then
                compute.attributes.occi!.compute!.state = "inactive"
                compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#restart|
              when "suspend" then
                compute.attributes.occi!.compute!.state = "suspended"
                compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#start|
              when "error" then
                compute.attributes.occi!.compute!.state = "error"
                compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#start|
              else
                compute.attributes.occi!.compute!.state = "inactive"
                compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#start|
            end
          end

          def register_all_instances(client)

            client.servers.each do |backend_object|
              parse_backend_object client, backend_object
            end
          end

          def deploy(client, compute, options = {})
            OCCI::Log.debug "Deploy #{compute.inspect}"

            compute.id = UUIDTools::UUID.timestamp_create.to_s

            os_tpl = compute.mixins.select { |mixin|
              mixin = @model.get_by_id(mixin) if mixin.kind_of? String
              mixin.related_to? 'http://schemas.ogf.org/occi/infrastructure#os_tpl' if mixin
            }.compact.first

            if os_tpl
              os_tpl = @model.get_by_id(os_tpl)
              image_ref = os_tpl.attributes.org.openstack.os_tpl.id['Default']
            end

            flavor_tpl = compute.mixins.select { |mixin|
              mixin = @model.get_by_id(mixin) if mixin.kind_of? String
              mixin.related_to? 'http://schemas.ogf.org/occi/infrastructure#resource_tpl' if mixin
            }.compact.first

            if flavor_tpl
              flavor_tpl = @model.get_by_id(flavor_tpl)
              flavor_id = flavor_tpl.term
            end

            image_ref ||= options[:default_image]
            flavor_id ||= options[:default_flavor]

            storage_endpoint = Config.instance.amqp[:identifier].split('://').last
            storage_endpoint = storage_endpoint.split('/').first
            storage_endpoint = storage_endpoint.split(':').first

            file_content = {
                :compute_uuid => compute.id,
                :storage_endpoint => storage_endpoint,
                :endpoint => Config.instance.amqp[:identifier].split('amqp.occi.').last
            }
            meta_data = {'occi_attribute_occi.core.id' => compute.id.to_s}

            if compute.attributes.cloud4e!.service!.simulation!.identifier
              identifier = compute.attributes.cloud4e!.service!.simulation!.identifier
              file_content[:service_identifier] = identifier
              meta_data['occi_attribute_cloud4e.service.simulation.identifier'] = identifier
            end

            file = {'contents' => file_content.to_yaml, 'path' => 'home/occi.info'}

            personality = []
            personality << file

            options = {
                'metadata' => meta_data,
                'user_data' => Base64.encode64(file_content.to_yaml),
                'adminPass' => 'cloud4e'
            }

            client.create_server compute.title, image_ref, flavor_id, options
          end

          def delete(client, compute)
            OCCI::Log.debug "Delete #{compute.inspect}"

            server_id =  compute.attributes.org!.openstack!.compute!.id

            client.delete_server(server_id)
          end

          def start(client, compute, parameters=nil)

          end

          def stop(client, compute, parameters=nil)

          end

          def restart(client, compute, parameters=nil)

          end

          def suspend(client, compute, parameters=nil)

          end
        end
      end
    end
  end
end