describe 'database' do
  def run_script(commands)
    raw_output = nil
    IO.popen("./cmake-build-debug/myDataBase", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it '插入并查询行' do
    result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit"
    ])
    expect(result).to match_array([
      "sql > 执行完毕",
      "sql > (1, user1, person1@example.com)",
      "执行完毕",
      "sql > ",
    ])
  end

  it '满表错误' do
    script = (1..1401).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result[-2]).to eq('sql > 错误：表已经满了')
  end

  it '允许使用最大长度的字段' do
    long_username = "a" * 32
    long_email = "a" * 255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "sql > 执行完毕",
      "sql > (1, #{long_username}, #{long_email})",
      "执行完毕",
      "sql > ",
    ])
  end

  it '如果输入字段太长会报错' do
    long_username = "a"*33
    long_email = "1" * 256
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "sql > 输入参数过长",
      "sql > 执行完毕",
      "sql > ",
    ])
  end

  it '如果id为负数则会报错' do
    script = [
      "insert -1 ankh ankh@ankh.com",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "sql > ID必须为非负数",
      "sql > 执行完毕",
      "sql > ",
    ])
  end
end
