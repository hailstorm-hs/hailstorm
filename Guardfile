require 'open3'
require 'colored'

notification :tmux,
  display_message: true

guard :shell do
  watch(/.*\.((l?hs)|cabal)$/) do |m|
    puts "Building hailstorm...".cyan
    cabal_command = "cabal build --ghc-options='-Wall -Werror'"
    Open3.popen3(cabal_command) do |_, stdout, stderr, wait_thr|
      if wait_thr.value != 0
        puts "FAILURE".red
        o = stdout.read
        e = stderr.read
        Notifier.notify("Build failed", {
          title: "Hailstorm",
          type: "failed"
        })
        puts o
        puts e.red
      else
        puts "SUCCESS".green
      end
    end
  end
end
