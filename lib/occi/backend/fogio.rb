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
# Description: fog.io Backend
# Author(s): Boris Parak, Florian Feldhaus, Maik Srba
##############################################################################

require 'occi/backend/manager'
require 'hashie/hash'
require 'occi/log'
require 'uuidtools'
require 'pstore'
require 'fog'

module OCCI
  module Backend

    # ---------------------------------------------------------------------------------------------------------------------         
    class Fogio < OCCI::Core::Resource

      attr_reader :model
      attr_accessor :amqp_worker

      def self.kind_definition
        kind = OCCI::Core::Kind.new('http://rocci.info/server/backend#', 'fogio')

        kind.related = %w{http://rocci.org/serer#backend}
        kind.title   = "rOCCI fog.io backend"

        kind.attributes.info!.rocci!.backend!.fogio!.provider!.Default     = 'openstack'
        kind.attributes.info!.rocci!.backend!.fogio!.provider!.Pattern     = '[a-zA-Z0-9_]*'
        kind.attributes.info!.rocci!.backend!.fogio!.provider!.Description = 'Management Middelware'

        kind.attributes.info!.rocci!.backend!.fogio!.endpoint!.Required = true

        kind.attributes.info!.rocci!.backend!.fogio!.scheme!.Default = 'http://my.occi.service/'

        kind
      end

      def initialize(kind='http://rocci.org/server#backend', mixins=nil, attributes=nil, links=nil)
        #TODO openstack_api_key and openstack_username per authorization
        @provider      = attributes.info.rocci.backend.fogio.provider
        @endpoint      = attributes.info.rocci.backend.fogio.endpoint
        @admin_token   = attributes.info.rocci.backend.fogio.token
        @tenant        = attributes.info.rocci.backend.fogio.tenant
        @user          = attributes.info.rocci.backend.fogio.user
        @api_key       = attributes.info.rocci.backend.fogio.api_key
        @default_image = attributes.info.rocci.backend.fogio.default_image

        #TODO make it independent from openstack

        @credentials = {
            :provider => @provider,
            :openstack_auth_url => @endpoint,
            :openstack_api_key => @api_key,
            :openstack_username => @user,
            #:openstack_auth_token => @admin_token,
            :openstack_tenant => @tenant,
        }

        scheme = attributes.info!.rocci!.backend!.fogio!.scheme if attributes
        scheme ||= self.class.kind_definition.attributes.info.rocci.backend.fogio.scheme.Default
        scheme.chomp!('/')
        @model = OCCI::Model.new
        @model.register_core
        @model.register_infrastructure
        @model.register_files("etc/backend/fogio/model/infrastructure/#{@provider}", scheme)
        @model.register_files("etc/backend/fogio/model/infrastructure/amqp", scheme)
        @model.register_files("etc/backend/fogio/model/service", scheme)
        @model.register_files("etc/backend/fogio/templates/#{@provider}", scheme)

        require "occi/backend/fogio/#{@provider}/compute"
        require "occi/backend/fogio/#{@provider}/network"
        require "occi/backend/fogio/#{@provider}/storage"
        require "occi/backend/fogio/#{@provider}/image"
        require "occi/backend/fogio/cloud4e/simulation"
        @compute = class_from_string("OCCI::Backend::Fogio::#{@provider.camelize}::Compute").new(@model)
        @network = class_from_string("OCCI::Backend::Fogio::#{@provider.camelize}::Network").new(@model)
        @storage = class_from_string("OCCI::Backend::Fogio::#{@provider.camelize}::Storage").new(@model)
        @images = class_from_string("OCCI::Backend::Fogio::#{@provider.camelize}::Image").new(@model)
        @simulation = class_from_string("OCCI::Backend::Fogio::Cloud4e::Simulation").new(@model)

        OCCI::Backend::Manager.register_backend(OCCI::Backend::Fogio, OCCI::Backend::Fogio::OPERATIONS)

        super(kind, mixins, attributes, links)
      end

      def class_from_string(str)
        str.split('::').inject(Object) do |mod, class_name|
          mod.const_get(class_name)
        end
      end

      def authorized?(username, password)
        #TODO make it intependent from openstack
        #test= Fog::OpenStack.authenticate_v2({:openstack_auth_token => @token, :openstack_auth_uri => URI.parse(@endpoint), :openstack_tenant => "rocci"})
        #test = test
      end

      # Generate a new fog.io client for the target User, if the username
      # is nil the Client is generated for the default user
      # @param [String] username
      # @return [Client]
      def client(username='default')
        username ||= 'default'

        @pstore = PStore.new(username)
        @pstore.transaction do
          @pstore['links']   ||= []
          @pstore['mixins']  ||= []
          @pstore['actions'] ||= []
        end

        #register saved mixins and actions
        @pstore.transaction(read_only=true) do
          actions = @pstore['actions']

          actions.each do |action|
            @model.register(action)
          end

          mixins = @pstore['mixins']

          mixins.each do |mixin|
            @model.register(mixin)
          end
        end

        #TODO return fogio client
        fog_client = Fog::Compute.new(@credentials)
        fog_client
      end

      #def get_username(cert_subject)
      #  cn = cert_subject [/.*\/CN=([^\/]*).*/,1]
      #  user = cn.downcase.gsub ' ','' if cn
      #  user ||= 'default'
      #end

      def get_username(subject, type="CERT")
        test = test
        case(type)
          when "KEYSTONE"
            @token = subject
            #@credentials = {:provider => @provider, :openstack_auth_url => @endpoint, :openstack_auth_token => subject, :openstack_tenant => @tenant}
            #TODO geht Username over check
            user ||= 'anonymous'

          else
            cn = cert_subject [/.*\/CN=([^\/]*).*/,1]
            user = cn.downcase.gsub ' ','' if cn
        end
        user ||= 'default'
      end

      # ---------------------------------------------------------------------------------------------------------------------
      # Operation mappings

      OPERATIONS = {}

      OPERATIONS["http://schemas.ogf.org/occi/infrastructure#compute"] = {

          # Generic resource operations
          :deploy => :compute_deploy,
          :update_state => :resource_update_state,
          :delete => :compute_delete,

          # network specific resource operations
          :start => :compute_action_start,
          :stop => :compute_action_stop,
          :restart => :compute_action_restart,
          :suspend => :compute_action_suspend
      }

      OPERATIONS["http://cloud4e.org/occi/service#simulation"] = {
          :deploy => :simulation_deploy,
          :delete => :simulation_delete
      }

      OPERATIONS["http://schemas.ogf.org/occi/infrastructure#amqplink"] = {
          :link   => :amqplink_link,
          :delete => :amqplink_delete,
          :amqp_call => :amqplink_call
      }

      OPERATIONS["http://schemas.ogf.org/occi/infrastructure#network"] = {

          # Generic resource operations
          :deploy => :network_deploy,
          :update_state => :resource_update_state,
          :delete => :resource_delete,

          # Network specific resource operations
          :up => :network_action_up,
          :down => :network_action_down
      }

      OPERATIONS["http://schemas.ogf.org/occi/infrastructure#storage"] = {

          # Generic resource operations
          :deploy => :storage_deploy,
          :update_state => :resource_update_state,
          :delete => :resource_delete,

          # Network specific resource operations
          :online => :storage_action_online,
          :offline => :storage_action_offline,
          :backup => :storage_action_backup,
          :snapshot => :storage_action_snapshot,
          :resize => :storage_action_resize
      }

      # ---------------------------------------------------------------------------------------------------------------------
      def register_existing_resources(client)

        @images.register_all client
        #@network.register_all_instances
        #@storage.register_all_instances
        @compute.register_all_instances client

        entities = []

        @pstore.transaction(read_only=true) do
          entities = @pstore['links']
        end

        entities.each do |entity|
          #Link zu seiner Resource hinzufügen
          add_actions_from_link(entity)
          if add_link_to_resource(entity)
            kind = @model.get_by_id(entity.kind)
            kind.entities << entity
          end
          OCCI::Log.debug("#### Number of entities in kind #{kind.type_identifier}: #{kind.entities.size}") if kind
        end
      end

      def add_actions_from_link(link)
        if link.mixins.any?
          #has mixins

          link.mixins.each do |key, value|
            mixin = @model.get_by_id key
            if mixin.actions.any?
              mixin.actions.each do |key2, value2|
                link.actions << key2
              end
            end
            link.actions.uniq!
          end
        end
      end

      def add_link_to_resource(link)
        source = link.source
        kind = @model.get_by_location(source.rpartition('/').first + '/')
        uuid = source.rpartition('/').last

        resource = (kind.entities.select { |entity| entity.id == uuid } if kind.entity_type == OCCI::Core::Resource).first

        if !resource.nil?
          resource.links << link
          true
        else
          #source does not exist
          amqplink_delete(@pstore, link)
          false
        end
      end

      # TODO: register user defined mixins

      def compute_deploy(client, compute)
        @compute.deploy(client, compute, :default_image => @default_image)
      end

      def compute_delete(client, compute)
        @compute.delete(client, compute)
      end

      def storage_deploy(client, storage)
        storage.id = UUIDTools::UUID.timestamp_create.to_s
        storage_action_online(client, storage)
        store(client, storage)
      end

      def simulation_deploy(client, simulation)
        @simulation.deploy(client, simulation)
      end

      def simulation_delete(client, simulation)
        @simulation.deploy(client, simulation)
      end

      def amqplink_link(client, amqplink)
        amqplink.id = UUIDTools::UUID.timestamp_create.to_s
        store_link amqplink
      end

      def amqplink_delete(client, amqplink)
        #link aus resource lösen und dann löschen
        store_link amqplink, true
      end

      def network_deploy(client, network)
        network.id = UUIDTools::UUID.timestamp_create.to_s
        network_action_up(client, network)
        store(client, network)
      end

      # ---------------------------------------------------------------------------------------------------------------------
      def store_link(link, delete = false)
        OCCI::Log.debug("### DUMMY: Deploying link with id #{link.id}")
        @pstore.transaction do
          @pstore['links'].delete_if { |res| res.id == link.id }
          @pstore['links'] << link unless delete
        end
      end

      def store_action(action, delete = false)
        @pstore.transaction do
          @pstore['actions'].delete_if { |res| res.type_identifier == action.type_identifier }
          @pstore['actions'] << action unless delete
        end
      end

      def store_mixin(mixin, delete = false)
        @pstore.transaction do
          @pstore['mixins'].delete_if { |res| res.type_identifier == mixin.type_identifier }
          @pstore['mixins'] << mixin unless delete
        end
      end

      # ---------------------------------------------------------------------------------------------------------------------
      def resource_update_state(resource)
        OCCI::Log.debug("Updating state of resource '#{resource.attributes['occi.core.title']}'...")
      end

      # ---------------------------------------------------------------------------------------------------------------------
      def resource_delete(client, resource)
        OCCI::Log.debug("Deleting resource '#{resource.attributes['occi.core.title']}'...")
        client.transaction do
          client['resources'].delete_if { |res| res.id == resource.id }
        end
      end

      # ---------------------------------------------------------------------------------------------------------------------
      # ACTIONS
      # ---------------------------------------------------------------------------------------------------------------------

      def compute_action_start(client, compute, parameters=nil)
        action_fogio(client, compute)
        compute.attributes.occi!.compute!.state = 'active'
        compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#stop http://schemas.ogf.org/occi/infrastructure/compute/action#restart http://schemas.ogf.org/occi/infrastructure/compute/action#suspend|
        store(client, compute)
      end

      def compute_action_stop(client, compute, parameters=nil)
        action_fogio(client, compute)
        compute.attributes.occi!.compute!.state = 'inactive'
        compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#start|
        store(client, compute)
      end

      def compute_action_restart(client, compute, parameters=nil)
        compute_action_start(client, compute)
      end

      def compute_action_suspend(client, compute, parameters=nil)
        action_fogio(client, compute)
        compute.attributes.occi!.compute!.state = 'suspended'
        compute.actions = %w|http://schemas.ogf.org/occi/infrastructure/compute/action#start|
        store(client, compute)
      end

      def storage_action_online(client, storage, parameters=nil)
        action_fogio(client, storage)
        storage.attributes.occi!.storage!.state = 'online'
        storage.actions = %w|http://schemas.ogf.org/occi/infrastructure/storage/action#offline http://schemas.ogf.org/occi/infrastructure/storage/action#restart http://schemas.ogf.org/occi/infrastructure/storage/action#suspend http://schemas.ogf.org/occi/infrastructure/storage/action#resize|
        store(client, storage)
      end

      def storage_action_offline(client, storage, parameters=nil)
        action_fogio(client, storage)
        storage.attributes.occi!.storage!.state = 'offline'
        storage.actions = %w|http://schemas.ogf.org/occi/infrastructure/storage/action#online http://schemas.ogf.org/occi/infrastructure/storage/action#restart http://schemas.ogf.org/occi/infrastructure/storage/action#suspend http://schemas.ogf.org/occi/infrastructure/storage/action#resize|
        store(client, storage)
      end

      def storage_action_backup(client, storage, parameters=nil)
        # nothing to do, state and actions stay the same after the backup which is instant for the fog.io
      end

      def storage_action_snapshot(client, storage, parameters=nil)
        # nothing to do, state and actions stay the same after the snapshot which is instant for the fog.io
      end

      def storage_action_resize(client, storage, parameters=nil)
        puts "Parameters: #{parameters}"
        storage.attributes.occi!.storage!.size = parameters[:size].to_i
        # state and actions stay the same after the resize which is instant for the fog.io
        store(client, storage)
      end

      def network_action_up(client, network, parameters=nil)
        action_fogio(client, network)
        network.attributes.occi!.network!.state = 'up'
        network.actions = %w|http://schemas.ogf.org/occi/infrastructure/network/action#down|
        store(client, network)
      end

      def network_action_down(client, network, parameters=nil)
        action_fogio(client, network)
        network.attributes.occi!.network!.state = 'down'
        network.actions = %w|http://schemas.ogf.org/occi/infrastructure/network/action#up|
        store(client, network)
      end

      def amqplink_call(client, amqplink, parameters=nil)
        #TODO Link muss angepasst werden
        amqp_target = amqplink.target
        queue       = amqplink.attributes.occi.amqplink.queue
        action      = parameters["action"]
        params      = parameters["parameters"]
        params.delete(:action)
        params.delete(:method)

        raise "No Amqp Producer is set" unless @amqp_worker

        path = amqplink.location + "?action=" + action.term

        params.each do |key, value|
          path += "&" + key.to_s + "=" + value.to_s
        end

        options = {
            :routing_key  => queue,
            :content_type => "application/occi+json",
            :type         => "post",
            :headers => {
                :path_info => path
            }
        }
        collection = OCCI::Collection.new
        collection.actions << action
        message = collection.to_json

        @amqp_worker.request(message, options)
        #TODO vergiss nicht das occi 2.5.16 gem mit den änderungen an dem parser -> link rel actions source target

      end

      def send_to_amqp(amqp_queue, resource, action, parameters)
        OCCI::Log.debug("Delegating action to amqp_queue: [#{amqp_queue}]")

        if @amqp_worker
          path = resource.location + "?action=" + parameters[:action]

          #alles ausser action und method
          parameters.each do |key, value|
            unless key.to_s == "action" || key.to_s == "method"
              path += "&" + key.to_s + "=" + value.to_s
            end
          end

          options = {
              :routing_key  => amqp_queue,
              :content_type => "application/occi+json",
              :type         => "post",
              :headers => {
                  :path_info => path
              }
          }
          collection   = OCCI::Collection.new
          collection.actions << action



          message = collection.to_json
          @amqp_worker.request(message, options)
          test = test
        end


      end

      # ---------------------------------------------------------------------------------------------------------------------
      def action_fogio(client, resource, parameters=nil)
        OCCI::Log.debug("Calling method for resource '#{resource.attributes['occi.core.title']}' with parameters: #{parameters.inspect}")
        resource.links ||= []
        resource.links.delete_if { |link| link.rel.include? 'action' }
      end

      def register_action(action)
        store_action action
        @model.register(action)
      end

      def unregister_action(action)
        raise "Not Implemented Yet"
      end

      def register_mixin(mixin)

        #convert actions from occi 3.0.x to occi 2.5.x
        actions = mixin.actions
        mixin.actions = []
        actions.each do |action|
          mixin.actions << (action.scheme + action.term)
        end

        store_mixin mixin
        @model.register(mixin)
      end

      def unregister_mixin(mixin)
        #search if mixin is in use
        found_mixin = false

        @model.get.kinds.each do |kind|
          break if found_mixin
          kind.entities.each do |entity|
            if entity.mixins.select{|smixin| smixin == mixin.type_identifier}.any?
              found_mixin = true
              break
            end
          end
        end

        unless found_mixin
          #unregister in Model
          @model.categories.delete mixin.type_identifier
          @model.locations.delete mixin.location
          store_mixin mixin, true

          mixin.actions.each do |action|
            #unregister actions
            action.type_identifier = (action.scheme + action.term)
            @model.categories.delete action.type_identifier
            #delete from pstore
            store_action action, true
          end
        end
      end

    end

  end
end