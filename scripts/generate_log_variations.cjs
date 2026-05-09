const fs = require('fs');

const originalLog = JSON.parse(fs.readFileSync('fixtures/events/log.json', 'utf8'));

// Helper to clone the original log and replace messages
function createVariation(filename, newMessages, finishReason = 'stop') {
  const newLog = JSON.parse(JSON.stringify(originalLog));
  newLog.event_id = filename.replace('.json', '');
  newLog.data.llm.messages = [
    originalLog.data.llm.messages[0], // Keep system prompt
    ...newMessages
  ];
  newLog.data.llm.finishReason = finishReason;
  fs.writeFileSync(`fixtures/events/${filename}`, JSON.stringify(newLog, null, 2));
  console.log(`Created fixtures/events/${filename}`);
}

// Variation 1: Excalidraw Venn Diagram
createVariation('log_variation_excalidraw.json', [
  {
    "content": "<agent-task agentType=\"canvas\">\nmedium: excalidraw\n\nDraw a comparative Venn diagram for Plant Cell vs Animal Cell.\n</agent-task>",
    "role": "user"
  },
  {
    "content": [
      {
        "type": "tool-call",
        "toolName": "Mcp_canvas_batch_create_elements",
        "toolCallId": "call_abc123",
        "input": {
          "elements": [
            { "type": "ellipse", "x": 100, "y": 100, "width": 300, "height": 300, "strokeColor": "#1e1e1e", "backgroundColor": "transparent" },
            { "type": "ellipse", "x": 250, "y": 100, "width": 300, "height": 300, "strokeColor": "#1e1e1e", "backgroundColor": "transparent" },
            { "type": "text", "x": 150, "y": 80, "text": "Plant Cell" },
            { "type": "text", "x": 380, "y": 80, "text": "Animal Cell" }
          ]
        }
      }
    ],
    "role": "assistant"
  },
  {
    "content": "<canvas-tool-result toolName=\"Mcp_canvas_batch_create_elements\" toolCallId=\"call_abc123\">\n{\n  \"type\": \"json\",\n  \"value\": {\n    \"success\": true,\n    \"createdIds\": [\"el_1\", \"el_2\", \"el_3\", \"el_4\"]\n  }\n}\n</canvas-tool-result>",
    "role": "user"
  }
], 'stop');

// Variation 2: Successful Code App Generation
createVariation('log_variation_code_success.json', [
  {
    "content": "<agent-task agentType=\"canvas\">\nmedium: code\n\nBuild a simple projectile motion simulator.\n</agent-task>",
    "role": "user"
  },
  {
    "content": [
      {
        "type": "tool-call",
        "toolName": "Bash",
        "toolCallId": "call_def456",
        "input": {
          "command": "mkdir -p /archil/canvas-apps/projectile-sim && cat << 'EOF' > /archil/canvas-apps/projectile-sim/server.ts\nconst html = `<html><body><h1>Projectile Sim</h1></body></html>`;\nBun.serve({port:Number(process.env.PORT||3000), fetch(){return new Response(html,{headers:{'content-type':'text/html'}})}});\nEOF\nbun /archil/canvas-apps/projectile-sim/server.ts"
        }
      }
    ],
    "role": "assistant"
  },
  {
    "content": "<canvas-tool-result toolName=\"Bash\" toolCallId=\"call_def456\">\n{\n  \"type\": \"json\",\n  \"value\": {\n    \"exitCode\": 0,\n    \"stdout\": \"Server running on port 3000\",\n    \"stderr\": \"\"\n  }\n}\n</canvas-tool-result>",
    "role": "user"
  },
  {
    "content": [
      {
        "type": "tool-call",
        "toolName": "Mcp_canvas_create_runtime_app_frame",
        "toolCallId": "call_ghi789",
        "input": {
          "appId": "projectile-sim",
          "title": "Projectile Motion Simulator",
          "width": 800,
          "height": 600
        }
      }
    ],
    "role": "assistant"
  },
  {
    "content": "<canvas-tool-result toolName=\"Mcp_canvas_create_runtime_app_frame\" toolCallId=\"call_ghi789\">\n{\n  \"type\": \"json\",\n  \"value\": {\n    \"success\": true,\n    \"frameId\": \"frame_1\"\n  }\n}\n</canvas-tool-result>",
    "role": "user"
  }
], 'stop');

// Variation 3: Image Generation Task
createVariation('log_variation_image_gen.json', [
  {
    "content": "<agent-task agentType=\"canvas\">\nmedium: generated_image\n\nGenerate a realistic textbook visual of a plant cell.\n</agent-task>",
    "role": "user"
  },
  {
    "content": [
      {
        "type": "tool-call",
        "toolName": "Mcp_canvas_generate_canvas_image",
        "toolCallId": "call_jkl012",
        "input": {
          "prompt": "A highly detailed, realistic textbook illustration of a plant cell, showing organelles like chloroplasts, nucleus, and cell wall clearly.",
          "x": 100,
          "y": 100,
          "width": 512,
          "height": 512
        }
      }
    ],
    "role": "assistant"
  },
  {
    "content": "<canvas-tool-result toolName=\"Mcp_canvas_generate_canvas_image\" toolCallId=\"call_jkl012\">\n{\n  \"type\": \"json\",\n  \"value\": {\n    \"success\": true,\n    \"imageId\": \"img_1\"\n  }\n}\n</canvas-tool-result>",
    "role": "user"
  }
], 'stop');
