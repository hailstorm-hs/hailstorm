require 'open3'
require 'colored'

guard :shell do
  watch(/.*\.((l?hs)|cabal)$/) do |m|
    puts "Building hailstorm...".cyan
    cabal_command = "cabal build --ghc-options='-Wall -Werror'"
    Open3.popen3(cabal_command) do |_, stdout, stderr, wait_thr|
      if wait_thr.value != 0
        puts "FAILURE".red
        o = stdout.read
        e = stderr.read
        puts o
        puts e.red
      else
        puts "SUCCESS".cyan
      end
    end
  end
end
