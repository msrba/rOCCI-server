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
        class Image
          attr_accessor :model

          def initialize(model)
            @model = model
          end

          def register_all(client)

            #client.servers.each do |backend_object|
            #  parse_backend_object client, backend_object
            #end
          end

          private
          def parse_backend_object(client, backend_object)
            related = %w|http://schemas.ogf.org/occi/infrastructure#os_tpl|
            term = backend_object['NAME'].downcase.chomp.gsub(/\W/, '_')
            # TODO: implement correct schema for service provider
            scheme = self.attributes.info.rocci.backend.openstack.scheme + "/occi/infrastructure/os_tpl#"
            title = backend_object['NAME']
            mixin = OCCI::Core::Mixin.new(scheme, term, title, nil, related)
            @model.register(mixin)
          end
        end
      end
    end
  end
end