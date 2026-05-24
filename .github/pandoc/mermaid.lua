local function escape_html(s)
  s = s:gsub("&", "&amp;")
  s = s:gsub("<", "&lt;")
  s = s:gsub(">", "&gt;")
  return s
end

function CodeBlock(el)
  if el.classes[1] == "mermaid" then
    return pandoc.RawBlock("html", '<div class="mermaid">\n' .. escape_html(el.text) .. '\n</div>')
  end
end
