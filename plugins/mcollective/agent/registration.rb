module MCollective
  module Agent
    # CouchDB discovery agent for The Marionette Collective
    #
    # Released under the Apache License, Version 2
    class Registration
      attr_reader :timeout, :meta

      def initialize
        config = Config.instance.pluginconf

        @timeout = 5
        @meta = {:license => "Apache License, Version 2",
                 :author => "Simon Croome <simon@croome.org>",
                 :timeout => @timeout,
                 :name => "CouchDB Discovery Agent",
                 :version => MCollective.version,
                 :url => "http://www.marionette-collective.org",
                 :description => "MCollective CouchDB Discovery Agent"}

        require 'couchrest'

        @host = config["registration.host"] || "localhost"
        @port = config["registration.port"] || "5984"
        @dbname = config["registration.db"] || "mcollective"
        @yaml_dir = config["registration.extra_yaml_dir"] || false

        Log.instance.info("Connecting to couchdb @ http://#{@host}:#{@port}/#{@dbname}")

        @db = CouchRest.database!("http://#{@host}:#{@port}/#{@dbname}")

        #@coll = @dbh.collection(@collection)
        #@coll.create_index("fqdn", {:unique => true, :dropDups => true})
      end

      def handlemsg(msg, connection)
        req = msg[:body]

        if (req.kind_of?(Array))
          Log.instance.warn("Got no facts - did you forget to add 'registration = Meta' to your server.cfg?");
          return nil
        end

        req[:fqdn] = req[:facts]["fqdn"]
        req[:lastseen] = Time.now.to_i

        # Optionally send a list of extra yaml files
        if (@yaml_dir != false)
          req[:extra] = {}
          Dir[@yaml_dir + "/*.yaml"].each do | f |
            req[:extra][File.basename(f).split('.')[0]] = YAML.load_file(f)
          end
        end

        # Sometimes facter doesnt send a fqdn?!
        if req[:fqdn].nil?
          Log.instance.debug("Got stats without a FQDN in facts")
          return nil
        end

        before = Time.now.to_f
        begin
          response = @db.save_doc({ :key          => req[:fqdn], 
                                    'identity'    => req[:identity],
                                    'agentlist'   => req[:agentlist], 
                                    'facts'       => req[:facts], 
                                    'classes'     => req[:classes], 
                                    'collectives' => req[:collectives], 
                                    'agentlist'   => req[:agentlist], 
                                    'lastseen'    => req[:lastseen]
                                 })
        rescue => e
          Log.error("%s: %s: %s" % [e.backtrace.first, e.class, e.to_s])
        ensure
          after = Time.now.to_f
          Log.instance.debug("Updated data for host #{req[:fqdn]} with id #{response['id']} in #{after - before}s")
        end

        nil
      end
    end
  end
end
