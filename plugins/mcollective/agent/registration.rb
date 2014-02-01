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
        @dbuser = config["registration.user"]
        @dbpass = config["registration.password"]
        @yaml_dir = config["registration.extra_yaml_dir"] || false

        if @dbuser && @dbpass
          dbauth = "#{@dbuser}:#{@dbpass}@"
        end

        Log.instance.info("Connecting to CouchDB @ http://#{dbauth}#{@host}:#{@port}/#{@dbname}")
        @db = CouchRest.database!("http://#{dbauth}#{@host}:#{@port}/#{@dbname}")

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

        doc = {
          '_id'         => req[:fqdn],
          'key'         => req[:fqdn],
          'identity'    => req[:identity],
          'agentlist'   => req[:agentlist],
          'facts'       => req[:facts],
          'classes'     => req[:classes],
          'collectives' => req[:collectives],
          'agentlist'   => req[:agentlist],
          'lastseen'    => req[:lastseen]
        }

        before = Time.now.to_f

        # If there's already a record with the same id, add the revision so
        # we update it rather than create a new record.
        begin
          result = @db.get(req[:fqdn])
          if result
            doc.merge!('_rev' => result['_rev'])
          end
        rescue
        end

        begin
          response = @db.save_doc(doc)
        rescue => e
          Log.error("%s: %s: %s" % [e.backtrace.first, e.class, e.to_s])
        ensure
          after = Time.now.to_f
          Log.instance.info((result ? "Updated" : "Inserted") + " data for host #{req[:fqdn]} with id #{response['id']} rev #{response['rev']} in #{after - before}s")
        end

        nil
      end
    end
  end
end
