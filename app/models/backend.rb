class Backend

  cattr_accessor :backend_class
  attr_reader :backend_class, :backend_instance, :options, :server_properties

  def initialize(options = {},
                 credentials = {},
                 server_properties = {})

    @options = options.freeze
    @credentials = credentials.freeze
    @server_properties = server_properties.freeze

    @backend_class = Backend.backend_class
    @backend_instance = Backend.backend_class.new(
      @options, @credentials, @server_properties
    )

    @backend_instance.extend(Backends::Helpers::MethodMissingHelper) unless @backend_instance.respond_to? :method_missing
  end

  def method_missing(m, *args, &block)
    raise Backends::Errors::MethodNotImplemented, "Method is not implemented in the backend model! [#{m}]"
  end

  include BackendApi::Compute
  include BackendApi::Network
  include BackendApi::Storage
  include BackendApi::OsTpl
  include BackendApi::ResourceTpl

end