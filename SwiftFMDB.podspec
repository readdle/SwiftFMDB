Pod::Spec.new do |s|

  s.name         = "SwiftFMDB"
  s.version      = "1.0.0"
  s.summary      = "SwiftFMDB"
  s.description  = <<-DESC
                  SwiftFMDB
                   DESC

  s.homepage     = "https://github.com/readdle/SwiftFMDB.git"
  s.license      = { :type => 'Copyright 2021 Readdle Inc.', :text => 'Copyright 2021 Readdle Inc.' }
  s.author       = { "Andrew Druk" => "adruk@readdle.com" }
  s.source       = { :git => "git@github.com:readdle/SwiftFMDB.git" }
  s.platforms    = { :ios => "10.0", :osx => "10.12" }


  s.source_files = "Sources/SwiftFMDB/*.swift"
  s.requires_arc = true

  s.dependency 'RDSQLite3'
  s.dependency 'Logging'

end
