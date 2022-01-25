
/*
In this tutorial we'll build a parser for simple CLI commands. Our parser will be able to parse numbers, words, and basic quoted strings, since those are the most common types one might encounter in simple CLI applications.

All our parser functions will follow the same pattern. They'll accept a string as input and return an array of token objects, plus any remaining input that didn't get consumed. Our finished function will take something like this:

echo "Hello World" 3.14

into this:

[ { type: 'word', value: 'echo' }, { type: 'string', value: 'Hello World' }, { type: 'number', value: 3.14 } ]

Let's do numbers first, because they are easiest!
*/
;(function () {
    function getNumber (input) {
        let i = 0
        while (i < input.length && '-.0123456789'.includes(input[i])) {
           i++
        }
        if (i === 0) return [[], input]
        let token = {
            type: 'number',
            value: Number(input.slice(0, i))
        }
        return [[token], input.slice(i)]
    }
    /*
    This getNumber() function takes a string as input, and has two outputs: a token array, and the remaining unused input. We'll see later on why we return an Array of tokens rather than just a single token. Let's see some examples.
    */
    // getNumber("123")
    // getNumber("1234 6789")
    // getNumber(" 12")
    /*
    The next type we'll add to our parser is strings. Strings are more complicated due to the quoting rules. The opening quote must match the end quote. To keep it simple, we won't deal with escaping characters with backslashes.
    */
    function getQuotedString (input) {
        if (input.length === 0) return [[], input]
        let i = 0
        let q = input[i++]
        if (!"'\"".includes(q)) return [[], input]
        while (i < input.length) {
           if (input[i] === q) break
           i++
        }
        let token = {
            type: 'string',
            value: input.slice(1, i)
        }
        return [[token], input.slice(i+1)]
    }
    // getQuotedString('hello')
    // getQuotedString('"hello"')
    // getQuotedString("'hello world'")
    /*
    Most developers wouldn't consider whitespace important, but why treat it any differently? We can parse it too!
    */
    function getWhitespace (input) {
        let i = 0
        while (i < input.length && ' \t\n\r'.includes(input[i])) {
           i++
        }
        if (i === 0) return [[], input]
        let token = {
            type: 'whitespace',
            value: input.slice(0, i)
        }
        return [[token], input.slice(i)]
    }
    // getWhitespace('hello  ')
    // getWhitespace(' hello')
    // getWhitespace('\thello')
    // getWhitespace('   \n   ')
    /*
    Finally, it is useful sometimes to have unquoted strings. That way we can parse identifiers, like command names, keywords, or constants. We could have stricter rules, like only alpha and underscore, but we'll just do "non-whitespace" and call it a raw string since it wasn't quoted.
    */
    function getRawString (input) {
        if (input.length === 0) return [[], input]
        let i = 0
        while (i < input.length && !' \t\n\r'.includes(input[i])) {
           i++
        }
        if (i===0) return [[], input]
        let token = {
            type: 'word',
            value: input.slice(0, i)
        }
        return [[token], input.slice(i)]
    }
    /*
    It's not very useful just grabbing a single token off the front of a string though. That's OK, I've secretly been planning this all along! The finished function combines all our parsers, trying each parser consecutively until the string is consumed. To prevent an infinite loop in case either we or the user made a mistake, we also check that the remaining input gets smaller each loop. If none of the parsers consumed any input and the text remaining isn't getting any smaller, we'll break.
    */
    function getTokens (input) {
        let parsers = [getNumber, getQuotedString, getRawString, getWhitespace]
        let result = []
        let t
        let lastsize = Infinity
        while (input.length > 0 && input.length < lastsize) {
            lastsize = input.length
            for (parser of parsers) {
                [t, input] = parser(input)
                result = [...result, ...t]
            }
        }
        return [result, input]
    }
    
    /*
    We built a fancy parser out of smaller specialized parsers! Notice how our final parser has the same calling convention and return types? We could continue creating more parsers on top of our existing parsers, such as an option parser to extract pairs ('-f', 'filename.txt') or lists.
    */
    // getTokens(`echo 'I say "hello"!' 1 2`)
    /*
    The kind of parser we just built is called a lexer, because the end result was just a flat list of tokens. In the next lesson, we'll see how we could tweak this formula to generate a hierarchical token structure commonly called an abstract syntax tree.
    */
    window.getTokens = getTokens
    })()