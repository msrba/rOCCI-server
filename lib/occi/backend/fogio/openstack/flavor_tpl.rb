module OCCI
  module Backend
    class Fogio
      module Openstack
        attr_accessor :model

        class FlavorTpl
          def initialize(model, backend)
            @model = model
            @backend = backend
          end

          def register_all(client)
            client.list_flavors.body['flavors'].each do |image|
              parse_backend_object client, image
            end
          end

          private
          def parse_backend_object(client, backend_object)
            related = %w|http://schemas.ogf.org/occi/infrastructure#resource_tpl|
            term = backend_object['id']

            scheme = @backend.scheme + "/occi/infrastructure/resource_tpl#"
            title = backend_object['name']
            mixin = OCCI::Core::Mixin.new(scheme, term, title, nil, related)
            @model.register(mixin)
          end
        end
      end
    end
  end
end